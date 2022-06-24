import abc
from typing import List, Tuple, Dict


class HashRepository(object, metaclass=abc.ABCMeta):
    def __init__(self):
        print("Hash repository parket...")
        super().__init__()
    
    @abc.abstractmethod
    def insert_hashes(self, song_id: int, hashes: List[Tuple[str, int]], batch_size: int = 1000) -> None:
        """
        Insert multiple hashes
        """
        pass

    @abc.abstractmethod
    def return_matches(self, hashes: List[Tuple[str, int]],
                       batch_size: int = 1000) -> Tuple[List[Tuple[int, int]], Dict[int, int]]:
        pass