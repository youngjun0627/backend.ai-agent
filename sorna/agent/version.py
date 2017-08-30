from typing import Tuple

VERSION_INFO: Tuple[int, int, int] = (0, 9, 14)
VERSION: str = '{}.{}.{}'.format(*VERSION_INFO)


if __name__ == '__main__':
    print(VERSION)
