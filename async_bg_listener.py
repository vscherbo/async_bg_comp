#!/usr/bin/env python
import argparse
import logging
import os
import queue
import signal
import sys
import threading
import time
from typing import Optional

import psycopg2
import psycopg2.extensions


# --- Функции для настройки логирования ---
def setup_logging(level: str, log_file: Optional[str] = None):
    """
    Настраивает логирование в файл или на консоль.
    :param level: Уровень логирования (например, 'DEBUG', 'INFO', 'WARNING', 'ERROR').
    :param log_file: Путь к файлу лога. Если None, логи идут в консоль.
    """
    LOG_FORMAT = '%(asctime)-15s | %(levelname)-7s | %(filename)-25s:%(lineno)4s - \
%(funcName)25s() | %(message)s'
    # Преобразуем строковый уровень в константу logging
    numeric_level = getattr(logging, level.upper(), logging.INFO)

    if not isinstance(numeric_level, int):
        raise ValueError(f'Invalid log level: {level}')
    handlers = []

    if log_file:
        # Создаем директорию для файла лога, если она не существует
        log_dir = os.path.dirname(os.path.abspath(log_file))

        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        handlers.append(logging.FileHandler(log_file, encoding='utf-8'))
    else:
        handlers.append(logging.StreamHandler(sys.stdout))  # По умолчанию в stdout
    logging.basicConfig(
        level=numeric_level,
        format=LOG_FORMAT,
        handlers=handlers,
        force=True  # Перенастраиваем, если logging уже был настроен
    )


# Логгер будет использовать конфигурацию из setup_logging
logger = logging.getLogger('NotifyListener')

# --- Класс слушателя ---


class NotificationListener:
    def __init__(self, db_uri: str, channel: str = 'do_bg_comp', max_workers: int = 5):
        """
        Инициализация слушателя уведомлений.
        :param db_uri: URI подключения к PostgreSQL
        :param channel: имя канала для подписки
        :param max_workers: максимальное количество потоков для обработки уведомлений
        """
        self.db_uri = db_uri
        self.channel = channel
        self.max_workers = max_workers
        self.notification_queue = queue.Queue()
        self.workers = []
        self.running = False
        self.conn: Optional[psycopg2.extensions.connection] = None
        # Событие для сигнализации о завершении работы
        self._stop_event = threading.Event()

    def _handle_notification(self, notification: psycopg2.extensions.Notify):
        """
        Обработка уведомления. Этот метод должен быть переопределен в подклассе.
        :param notification: объект уведомления
        """
        logger.info(f"Got notification on channel {notification.channel}: {notification.payload}")
        # Здесь должна быть ваша логика обработки уведомления
        # Пример:
        try:
            # Создаем отдельное соединение для обработчика, как обсуждалось ранее
            # Это делает обработку более устойчивой к сбоям соединения в других частях
            handler_conn = None
            try:
                handler_conn = psycopg2.connect(self.db_uri)
                # handler_conn.set_isolation_level(...) если нужно
                hndl_cursor = handler_conn.cursor()
                hndl_cursor.callproc('arc_energo.bg_comp',
                                     {'arg_id': notification.payload})
                results = hndl_cursor.fetchall()
                logger.info(f"Processed notification: {notification.payload}, results={results}")
                # handler_conn.commit() если isolation level не autocommit и были изменения
            finally:
                if handler_conn and not handler_conn.closed:
                    try:
                        handler_conn.close()
                    except (psycopg2.Error, OSError) as e:  # Исправлено: конкретные исключения
                        logger.debug(f"Error closing handler connection: {e}", exc_info=True)
        except Exception as e:
            logger.error(f"Error processing notification: {e}", exc_info=True)

    def _worker_loop(self):
        """
        Цикл обработки уведомлений в рабочем потоке.
        """

        while self.running or not self.notification_queue.empty():
            try:
                # Используем timeout для проверки флага running
                notification = self.notification_queue.get(timeout=1)

                if notification is not None:
                    self._handle_notification(notification)
                self.notification_queue.task_done()
            except queue.Empty:
                continue  # Проверим running снова
            except Exception as e:
                logger.error(f"Worker error: {e}", exc_info=True)

    def _listen_loop(self):
        """
        Основной цикл прослушивания уведомлений.
        """
        reconnect_delay = 1.0
        reconnect_backoff = 2.0
        max_reconnect_delay = 60.0

        while self.running:
            try:
                logger.info("Attempting to connect to database...")
                # Устанавливаем соединение
                self.conn = psycopg2.connect(self.db_uri)
                self.conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
                cursor = self.conn.cursor()
                cursor.execute(f"LISTEN {self.channel};")
                logger.info(f"Successfully connected and listening to channel '{self.channel}'...")
                reconnect_delay = 1.0  # Сброс задержки при успешном подключении

                while self.running:
                    # Ожидаем уведомлений с проверкой флага running
                    # Используем poll() и проверяем флаг running регулярно
                    try:
                        # poll() может блокировать, поэтому используем select/poll
                        # Но для простоты и совместимости оставим time.sleep и poll()
                        # с коротким циклом. Для более высокой отзывчивости
                        # можно использовать select.select() или select.poll()
                        # на self.conn.fileno(), но это сложнее.
                        self.conn.poll()
                    except psycopg2.OperationalError:
                        # poll() может выбросить OperationalError при разрыве
                        raise  # Перехватим ниже как ошибку соединения

                    # Обрабатываем все полученные уведомления

                    while self.conn.notifies:
                        notify = self.conn.notifies.pop(0)
                        # Проверяем running перед добавлением в очередь

                        if self.running:
                            self.notification_queue.put(notify)
                        else:
                            logger.debug("Ignoring notification, listener is stopping.")
                    # time.sleep(0.1) # Или используйте select/poll для более отзывчивости
                    # Вместо time.sleep используем wait с timeout, чтобы быстрее реагировать на stop

                    if self._stop_event.wait(0.1):
                        break  # Получен сигнал остановки

            except (psycopg2.OperationalError, psycopg2.InterfaceError,  # Ошибки psycopg2
                    ConnectionResetError, ConnectionAbortedError, BrokenPipeError,
                    OSError) as e:  # Исправлено: конкретные исключения
                logger.warning(f"Database connection lost or failed: {e}")

                if self.conn and not self.conn.closed:
                    try:
                        self.conn.close()
                    except (psycopg2.Error, OSError) as close_e:  # Исправлено: конкретные исключения
                        logger.debug(
                            f"Error closing connection during reconnect: {close_e}", exc_info=True)
                    self.conn = None

                if not self.running:
                    break  # Если остановка запрошена, не пытаемся переподключиться
                # Логика повтора с экспоненциальной задержкой
                logger.info(f"Reconnecting in {reconnect_delay:.2f} seconds...")
                time.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * reconnect_backoff, max_reconnect_delay)
            except Exception as e:
                logger.error(f"Unexpected error in listen loop: {e}", exc_info=True)

                if self.running:  # Останавливаем только если еще не остановлены
                    self.stop()

                break
            finally:
                if self.conn and not self.conn.closed:
                    try:
                        self.conn.close()
                    except (psycopg2.Error, OSError) as e:  # Исправлено: конкретные исключения
                        logger.debug(
                            f"Error closing database connection in finally: {e}", exc_info=True)
                    self.conn = None

    def start(self):
        """
        Запуск слушателя и рабочих потоков.
        """

        if self.running:
            logger.warning("Listener is already running")

            return
        self.running = True
        self._stop_event.clear()  # Сбрасываем событие
        # Запускаем рабочие потоки

        for i in range(self.max_workers):
            worker = threading.Thread(
                target=self._worker_loop,
                name=f"NotificationWorker-{i}",
                daemon=True  # Демонизируем потоки
            )
            worker.start()
            self.workers.append(worker)
            logger.info(f"Started worker thread {worker.name}")
        # Запускаем поток слушателя
        self.listener_thread = threading.Thread(
            target=self._listen_loop,
            name="NotificationListener",
            daemon=True  # Демонизируем поток слушателя
        )
        self.listener_thread.start()
        logger.info("Started listener thread")

    def stop(self):
        """
        Остановка слушателя и рабочих потоков.
        """

        if not self.running:
            return
        logger.info("Stopping listener...")
        self.running = False
        self._stop_event.set()  # Устанавливаем событие для пробуждения слушателя

        # Ожидаем завершения потока слушателя

        if hasattr(self, 'listener_thread') and self.listener_thread.is_alive():
            self.listener_thread.join(timeout=10)  # Увеличен timeout

            if self.listener_thread.is_alive():
                logger.warning("Listener thread did not stop gracefully")
        # Ожидаем завершения рабочих потоков
        # Ожидаем завершения обработки текущих задач в очереди
        self.notification_queue.join()  # Блокирует до тех пор, пока task_done()
        # не будет вызван для всех элементов

        for worker in self.workers:
            if worker.is_alive():
                worker.join(timeout=5)

                if worker.is_alive():
                    logger.warning(f"Worker thread {worker.name} did not stop gracefully")
        # Закрываем соединение, если оно еще открыто (на всякий случай)

        if self.conn and not self.conn.closed:
            try:
                self.conn.close()
            except (psycopg2.Error, OSError) as e:  # Исправлено: конкретные исключения
                logger.debug(f"Error closing connection in stop: {e}", exc_info=True)
        logger.info("Listener stopped")

    def __enter__(self):
        self.start()

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

# --- Функция разбора аргументов командной строки ---


def parse_arguments():
    """Парсит аргументы командной строки."""
    parser = argparse.ArgumentParser(
        description="Слушатель уведомлений PostgreSQL (LISTEN/NOTIFY).",
        formatter_class=argparse.RawTextHelpFormatter  # Для корректного отображения \n в help
    )
    parser.add_argument(
        '--db-uri', '-d',
        type=str,
        required=True,  # Сделаем обязательным
        help='URI подключения к PostgreSQL.\nПример: postgresql://user:password@host:port/database'
    )
    parser.add_argument(
        '--log-level', '-l',
        type=str,
        default='INFO',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        help='Уровень логирования. По умолчанию: INFO'
    )
    parser.add_argument(
        '--log-file', '-f',
        type=str,
        default=None,
        help='Путь к файлу лога. Если не указан, логи выводятся в консоль (stdout).'
    )
    parser.add_argument(
        '--channel', '-c',
        type=str,
        default='do_bg_comp',
        help='Имя канала PostgreSQL для LISTEN. По умолчанию: do_bg_comp'
    )
    parser.add_argument(
        '--workers', '-w',
        type=int,
        default=5,
        help='Количество рабочих потоков для обработки уведомлений. По умолчанию: 5'
    )

    return parser.parse_args()


# --- Глобальная переменная для хранения экземпляра слушателя ---
# Это нужно для доступа к нему из обработчика сигнала
_listener_instance: Optional[NotificationListener] = None


def signal_handler(signum, frame):
    """Обработчик сигналов SIGINT и SIGTERM."""
    signame = signal.Signals(signum).name
    logger.info(f"Received signal {signum} ({signame}). Initiating graceful shutdown...")

    if _listener_instance:
        # Вызываем stop на экземпляре слушателя
        # Это безопасно, так как stop() проверяет self.running
        _listener_instance.stop()
    # sys.exit(0) не рекомендуется использовать внутри обработчика сигнала
    # Лучше изменить флаг в основном цикле
    # Но в данном случае мы полагаемся на остановку слушателя,
    # которая должна привести к выходу из основного цикла


# --- Основная точка входа ---

if __name__ == "__main__":
    # 1. Парсим аргументы
    args = parse_arguments()

    # 2. Настраиваем логирование
    try:
        setup_logging(args.log_level, args.log_file)
        logger.info("Logging configured.")
    except Exception as e:
        print(f"Failed to configure logging: {e}", file=sys.stderr)
        sys.exit(1)

    # 3. Регистрируем обработчики сигналов
    # signal.signal(signal.SIGINT, signal_handler) # Обычно перехватывается KeyboardInterrupt
    signal.signal(signal.SIGTERM, signal_handler)
    # Для systemd SIGTERM является стандартным сигналом остановки
    # SIGINT (Ctrl+C) будет работать как и раньше, вызывая KeyboardInterrupt

    # 4. Создаем и запускаем слушатель
    listener = NotificationListener(
        db_uri=args.db_uri,
        channel=args.channel,
        max_workers=args.workers
    )
    # Сохраняем ссылку на экземпляр для обработчика сигнала
    _listener_instance = listener

    try:
        listener.start()
        logger.info(
            f"Notification listener started for channel '{args.channel}'. Press Ctrl+C to stop.")
        # Основной цикл ожидания. Выход будет либо по KeyboardInterrupt (Ctrl+C),
        # либо по завершению работы слушателя (например, из-за сигнала)

        while listener.running:  # Проверяем флаг running
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Received KeyboardInterrupt, stopping...")
    finally:
        # Гарантируем остановку слушателя
        listener.stop()
        _listener_instance = None  # Очищаем ссылку
