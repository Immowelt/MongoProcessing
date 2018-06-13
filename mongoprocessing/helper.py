import logging

_logger = None


def get_log():
    global _logger
    if _logger is None:
        _logger = logging.getLogger('mongoprocessing')
        _logger.setLevel(logging.DEBUG)

        fh = logging.FileHandler('mongoprocessing.log')
        fh.setLevel(logging.DEBUG)

        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)

        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)

        _logger.addHandler(fh)
        _logger.addHandler(ch)

    return _logger
