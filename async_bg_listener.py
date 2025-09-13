#!/usr/bin/env python

import logging
import queue
import threading
import time
from typing import Optional

import psycopg2
import psycopg2.extensions

# Настройка логирования
LOG_FORMAT = \
    '%(asctime)-15s | %(levelname)-7s | %(filename)-25s:%(lineno)4s - %(funcName)25s() | %(message)s'
logging.basicConfig(
    level=logging.INFO,
    # format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    format=LOG_FORMAT
)

logger = logging.getLogger('NotifyListener')


class NotificationListener:
    def __init__(self, db_uri: str, channel: str = 'do_bg_comp', max_workers: int = 5,
                 reconnect_attempts: int = 5, reconnect_delay: float = 1.0,
                 reconnect_backoff: float = 2.0):
        """
        Инициализация слушателя уведомлений.

        :param db_uri: URI подключения к PostgreSQL
        :param channel: имя канала для подписки
        :param max_workers: максимальное количество потоков для обработки уведомлений
        :param reconnect_attempts: попыток подключения
        :param reconnect_delay: задержка между попытками
        :param reconnect_backoff:
        """
        self.db_uri = db_uri
        self.channel = channel
        self.max_workers = max_workers
        self.reconnect_attempts = reconnect_attempts
        self.reconnect_delay = reconnect_delay
        self.reconnect_backoff = reconnect_backoff
        self.notification_queue = queue.Queue()
        self.workers = []
        self.running = False
        self.conn: Optional[psycopg2.extensions.connection] = None

    def _handle_notification_simple(self, notification: psycopg2.extensions.Notify):
        """
        Обработка уведомления. Этот метод должен быть переопределен в подклассе.

        :param notification: объект уведомления
        """
        logger.info(f"Got notification on channel {notification.channel}: {notification.payload}")
        # Здесь должна быть ваша логика обработки уведомления
        # Пример:
        try:
            # обработка
            # time.sleep(10)
            hndl_cursor = self.conn.cursor()
            hndl_cursor.callproc('arc_energo.bg_comp',
                                 {'arg_id': notification.payload})
            results = hndl_cursor.fetchall()
            logger.info(f"Processed notification: {notification.payload}, \
            results={results}")
        except Exception as e:
            logger.error(f"Error processing notification: {e}")

    def _handle_notification(self, notification: psycopg2.extensions.Notify,
                             conn: psycopg2.extensions.connection):
        logger.info(f"Got notification on channel {notification.channel}: {notification.payload}")
        try:
            hndl_cursor = conn.cursor()
            hndl_cursor.callproc('arc_energo.bg_comp', {'arg_id': notification.payload})
            results = hndl_cursor.fetchall()  # Или fetchone() если ожидается одна запись
            logger.info(f"Processed notification: {notification.payload}, results={results}")
            # conn.commit() если isolation level не autocommit и были изменения
        except Exception as e:
            logger.error(f"Error in _handle_notification for payload {notification.payload}: {e}")
            # Здесь можно реализовать повторную попытку, логирование или отправку в очередь DLQ
            raise  # Перебрасываем исключение, чтобы _worker_loop мог его обработать

    def _worker_loop(self):
        """
        Цикл обработки уведомлений в рабочем потоке.
        """

        while self.running or not self.notification_queue.empty():
            try:
                notification = self.notification_queue.get(timeout=1)

                if notification is not None:
                    # Создаем отдельное соединение для обработчика
                    handler_conn = None
                    try:
                        handler_conn = psycopg2.connect(self.db_uri)
                        # handler_conn.set_isolation_level(...) если нужно
                        self._handle_notification(notification, handler_conn)  # Передаем соединение
                    except Exception as e:
                        logger.error(f"Error processing notification {notification.payload}: {e}")
                        # Возможно, повторные попытки или отправка в очередь ошибок
                    finally:
                        if handler_conn and not handler_conn.closed:
                            try:
                                handler_conn.close()
                            except (psycopg2.Error, OSError) as e:
                                # Логируем ошибку закрытия, но не прерываем выполнение
                                logger.debug(
                                    f"Error closing handler connection: {e}", exc_info=True)
                            # Или, если быть совсем точным в типах исключений:
                            # except (psycopg2.OperationalError, psycopg2.InterfaceError,
                            #         psycopg2.InternalError, OSError) as e:
                            #     logger.debug(f"Error closing handler connection: {e}",
                            # exc_info=True)
                    # ---
                self.notification_queue.task_done()
            except queue.Empty:
                continue
            except Exception as e:
                logger.error(f"Worker error: {e}")

    def _listen_loop(self):
        """
        Основной цикл прослушивания уведомлений.
        """
        # Примерная структура внутри _listen_loop
        delay = self.reconnect_delay

        while self.running:
            try:
                # --- Блок подключения ---
                logger.info("Attempting to connect to database...")
                self.conn = psycopg2.connect(self.db_uri)
                self.conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
                cursor = self.conn.cursor()
                cursor.execute(f"LISTEN {self.channel};")
                logger.info(f"Successfully connected and listening to channel '{self.channel}'...")
                delay = self.reconnect_delay  # Сброс задержки при успешном подключении
                # --- Конец блока подключения ---

                # --- Основной цикл опроса ---

                while self.running:
                    self.conn.poll()  # Может выбросить исключение при разрыве

                    while self.conn.notifies:
                        notify = self.conn.notifies.pop(0)
                        self.notification_queue.put(notify)
                    time.sleep(0.1)  # Или используйте select/poll для более отзывчивости
                # --- Конец цикла опроса ---

            except (psycopg2.OperationalError, psycopg2.InterfaceError,  # Ошибки psycopg2
                    # Ошибки сети
                    ConnectionResetError, ConnectionAbortedError, BrokenPipeError) as e:
                logger.warning(f"Database connection lost or failed: {e}")

                if self.conn and not self.conn.closed:
                    try:
                        self.conn.close()
                    except (psycopg2.Error, OSError) as e:
                        # Логируем ошибку закрытия, но не прерываем выполнение
                        logger.debug(f"Error closing database connection: {e}", exc_info=True)
                    # Или, если быть совсем точным в типах исключений:
                    # except (psycopg2.OperationalError, psycopg2.InterfaceError,
                    #         psycopg2.InternalError, OSError) as e:
                    #     logger.debug(f"Error closing database connection: {e}", exc_info=True)

                    self.conn = None

                if not self.running:
                    break  # Если остановка запрошена, не пытаемся переподключиться

                # Логика повтора
                attempt = 0

                while attempt < self.reconnect_attempts and self.running:
                    logger.info(
                        f"Reconnect attempt {attempt + 1}/{self.reconnect_attempts} \
in {delay: .2f} seconds...")
                    time.sleep(delay)
                    # Сброс delay и attempt происходит только при успешном подключении выше
                    # Ограничение максимальной задержки
                    delay = min(delay * self.reconnect_backoff, 60.0)
                    attempt += 1

                    # Выходим из внутреннего цикла попыток, чтобы снова попытаться подключиться
                    # во внешнем цикле

                    break
                else:
                    if self.running:
                        logger.error(
                            f"Failed to reconnect after {self.reconnect_attempts} attempts. \
Stopping listener.")
                        self.stop()  # Или установить флаг ошибки

                    break  # Останавливаем внешний цикл

            except Exception as e:
                logger.error(f"Unexpected error in listen loop: {e}", exc_info=True)

                if self.running:  # Останавливаем только если еще не остановлены
                    self.stop()

                break
            finally:
                if self.conn and not self.conn.closed:
                    try:
                        self.conn.close()
                    except (psycopg2.Error, OSError) as e:
                        # Логируем ошибку закрытия, но не прерываем выполнение
                        logger.debug(f"Error closing database connection: {e}", exc_info=True)
                    # Или, если быть совсем точным в типах исключений:
                    # except (psycopg2.OperationalError, psycopg2.InterfaceError,
                    #         psycopg2.InternalError, OSError) as e:
                    #     logger.debug(f"Error closing database connection: {e}", exc_info=True)

                    self.conn = None

    def start(self):
        """
        Запуск слушателя и рабочих потоков.
        """

        if self.running:
            logger.warning("Listener is already running")

            return

        self.running = True

        # Запускаем рабочие потоки

        for i in range(self.max_workers):
            worker = threading.Thread(
                target=self._worker_loop,
                name=f"NotificationWorker-{i}",
                daemon=True
            )
            worker.start()
            self.workers.append(worker)
            logger.info(f"Started worker thread {worker.name}")

        # Запускаем поток слушателя
        self.listener_thread = threading.Thread(
            target=self._listen_loop,
            name="NotificationListener",
            daemon=True
        )
        self.listener_thread.start()
        logger.info("Started listener thread")

    def stop(self):
        """
        Остановка слушателя и рабочих потоков.
        """

        if not self.running:
            return

        self.running = False
        logger.info("Stopping listener...")

        # Ожидаем завершения потока слушателя

        if hasattr(self, 'listener_thread') and self.listener_thread.is_alive():
            self.listener_thread.join(timeout=5)

            if self.listener_thread.is_alive():
                logger.warning("Listener thread did not stop gracefully")

        # Ожидаем завершения рабочих потоков

        for worker in self.workers:
            if worker.is_alive():
                worker.join(timeout=5)

                if worker.is_alive():
                    logger.warning(f"Worker thread {worker.name} did not stop gracefully")

        # Закрываем соединение, если оно еще открыто

        if self.conn and not self.conn.closed:
            self.conn.close()

        logger.info("Listener stopped")

    def __enter__(self):
        self.start()

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()


if __name__ == "__main__":
    # Пример использования
    DB_URI = "postgresql://arc_energo@vm-pg-clone/arc_energo"

    # Создаем и запускаем слушатель
    listener = NotificationListener(db_uri=DB_URI, max_workers=10)

    try:
        listener.start()
        logger.info("Notification listener started. Press Ctrl+C to stop.")

        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Received KeyboardInterrupt, stopping...")
    finally:
        listener.stop()
