import os
import shutil
import sys
import csv
from enum import Enum
from typing import List
import heapq

"""
Program joins 2 files using specified by user column name and join_type.
Firstly it sorts csv files by specified column using external sort which splits file into (file size/BUFFER_SIZE) files
and then merges them using min heap. At the end, script joins 2 files and prints result iterating over them.
Program stores in memory at once BUFFER_SIZE characters (+one possible line from csv) while splitting into blocks
and (file size/BUFFER_SIZE)*(line size) while merging.
Time Complexity of sorting in blocks is O(F/B * BlogB) = O(F*logB), merging blocks O(N*log(F/B)),
reading files O(M) and joining + printing O(M) where B is a BUFFER_SIZE, F is a csv file size, M is amount of
characters in csv file and N is a amount of lines in csv file.
So final complexity is  O(F*logB + M + N*log(size/BUFFER_SIZE))
"""
BUFFER_SIZE = 1000


def main():
    if sys.argv[1] != "join":
        exit(1)
    file_path_left = sys.argv[2]
    file_path_right = sys.argv[3]
    column_name = sys.argv[4]
    join_type = sys.argv[5]
    if join_type == "left":
        join_type = JoinType.LEFT
    elif join_type == "right":
        join_type = JoinType.RIGHT
    elif join_type == "inner":
        join_type = JoinType.INNER
    else:
        print("Wrong join type")
        exit(1)

    joined_column_left = get_joined_column_id(file_path_left, column_name)
    joined_column_right = get_joined_column_id(file_path_right, column_name)

    external_sort(file_path_left, file_path_right, joined_column_left, joined_column_right)

    print_column_names(file_path_left, file_path_right)
    join(join_type, joined_column_left, joined_column_right)

    shutil.rmtree('./tmp/')


""" utilities """


def get_joined_column_id(file_path: str, column_name: str) -> int:
    with open(file_path) as file:
        reader = csv.reader(file, delimiter=';')
        column_names = next(reader)

        for i, name in enumerate(column_names):
            if name == column_name:
                return i

        print(f"{column_name} is a wrong column name")
        exit(1)


def save_block(block_id: int, block: List[List[str]], saving_path: str):
    file_path = "./tmp/block"+str(block_id)+saving_path+".csv"
    with open(file_path, 'x', newline='') as file:
        writer = csv.writer(file,  delimiter=';')
        writer.writerows(block)


def print_column_names(file_l: str, file_r: str):
    column_names = []
    with open(file_l) as file:
        reader = csv.reader(file, delimiter=';')
        column_names += next(reader)
    with open(file_r) as file:
        reader = csv.reader(file, delimiter=';')
        column_names += next(reader)
    print(column_names)


""" sorting csv files """


def external_sort(left_path: str, right_path: str, joined_column_left: int, joined_column_right: int):
    if os.path.exists('./tmp/'):
        shutil.rmtree('./tmp/')
    os.mkdir('./tmp/')
    block_number_left = save_sorted_blocks(left_path, joined_column_left, "left")
    block_number_right = save_sorted_blocks(right_path, joined_column_right, "right")
    merge_blocks(block_number_left, joined_column_left, "left")
    merge_blocks(block_number_right, joined_column_right, "right")


def save_sorted_blocks(file_path: str, joined_column: int, saving_path: str) -> int:
    current_block = []
    block_id = 0
    characters = 0

    with open(file_path) as file:
        csv_reader = csv.reader(file, delimiter=';')
        next(csv_reader)  # ignoring header
        for row in csv_reader:
            for item in row:
                characters += len(item)
            current_block.append(row)
            if characters >= BUFFER_SIZE:
                current_block.sort(key=lambda x: x[joined_column])
                save_block(block_id, current_block, saving_path)
                block_id += 1
                characters = 0
                current_block = []
        if characters > 0:
            current_block.sort(key=lambda x: x[joined_column])
            save_block(block_id, current_block, saving_path)
            block_id += 1

    return block_id


def merge_blocks(block_number: int, joined_column: int, saving_path: str):
    with open("./tmp/sorted_"+saving_path+".csv", 'x', newline='') as sorted_file:
        min_heap = []
        heapq.heapify(min_heap)
        open_files = []
        readers = []
        writer = csv.writer(sorted_file, delimiter=';')
        for i in range(block_number):
            file = open("./tmp/block"+str(i)+saving_path+".csv", 'r', newline='')
            reader = csv.reader(file, delimiter=';')

            readers.append(reader)
            open_files.append(file)

            row = next(reader)
            heapq.heappush(min_heap, (row[joined_column], row, i))

        while len(min_heap) > 0:
            min_element = heapq.heappop(min_heap)
            writer.writerow(min_element[1])
            row = None
            for row in readers[min_element[2]]:
                break
            if row:
                heapq.heappush(min_heap, (row[joined_column], row, min_element[2]))
            else:
                open_files[min_element[2]].close()


""" join """


class JoinType(Enum):
    LEFT = 0
    RIGHT = 1
    INNER = 2


def join(join_type: JoinType, joined_column_left: int, joined_column_right: int):
    with open("./tmp/sorted_left.csv") as left:
        with open("./tmp/sorted_right.csv") as right:
            # If it is a right join we can just change left and right file
            # to assume that it is always a left join
            if join_type == JoinType.RIGHT:
                left, right = right, left
                joined_column_right, joined_column_left = joined_column_left, joined_column_right
            reader_left = csv.reader(left, delimiter=';')
            reader_right = csv.reader(right, delimiter=';')
            row_right, should_right_read = get_next(reader_right)
            row_left, should_left_read = get_next(reader_left)

            right_len = len(row_right)

            while should_right_read and should_left_read:
                if row_left[joined_column_left] == row_right[joined_column_right]:
                    if join_type == JoinType.RIGHT:
                        print(row_right+row_left)
                    else:
                        print(row_left + row_right)
                    row_right, should_right_read = get_next(reader_right)
                    row_left, should_left_read = get_next(reader_left)
                elif row_left[joined_column_left] < row_right[joined_column_right]:
                    if join_type != JoinType.INNER:
                        if join_type == JoinType.LEFT:
                            print(row_left+[None for _ in range(right_len)])
                        else:
                            print([None for _ in range(right_len)] + row_left)
                    row_left, should_left_read = get_next(reader_left)
                else:
                    row_right, should_right_read = get_next(reader_right)

            while join_type != JoinType.INNER and should_left_read:
                if join_type == JoinType.LEFT:
                    print(row_left + [None for _ in range(right_len)])
                else:
                    print([None for _ in range(right_len)] + row_left)
                row_left, should_left_read = get_next(reader_left)


def get_next(reader: csv.reader) -> (List[str], bool):
    should_read = True
    row = []
    try:
        row = next(reader)
    except StopIteration:
        should_read = False
    return row, should_read


if __name__ == "__main__":
    main()
