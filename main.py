import os
import shutil
import sys
import csv
from typing import List
import heapq

"""Buffer size determines how many characters we can handle at one time """
BUFFER_SIZE = 20

def main():
    # file_path_left = sys.argv[2]
    # file_path_right = sys.argv[3]
    # column_name = sys.argv[4]
    # join_type = sys.argv[5]

    file_path_left = "left.csv"
    column_name = "Surname"

    joined_column_left = get_joined_column_id(file_path_left, column_name)
    # joined_column_right = get_joined_column_id(file_path_right, column_name)

    external_sort(file_path_left, joined_column_left)


##### utilities ########
def get_joined_column_id(file_path: str, column_name: str) -> int:
    with open(file_path) as file:
        reader = csv.reader(file, delimiter=';')
        column_names = next(reader)

        for i, name in enumerate(column_names):
            if name == column_name:
                return i

        print(f"{column_name} is a wrong column name")
        exit(1)


def save_block(block_id: int, block: List):
    file_path = "./tmp/block"+str(block_id)+".csv"
    with open(file_path, 'x', newline='') as file:
        writer = csv.writer(file,  delimiter=';')
        writer.writerows(block)


########### sorting csv files ###########
def external_sort(file_path: str, joined_column: int):
    if os.path.exists('./tmp/'):
        shutil.rmtree('./tmp/')
    os.mkdir('./tmp/')
    block_number = save_sorted_blocks(file_path, joined_column)
    merge_blocks(block_number, joined_column)


def save_sorted_blocks(file_path: str, joined_column: int) -> int:
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
                save_block(block_id, current_block)
                block_id += 1
                characters = 0
                current_block = []
        if characters > 0:
            current_block.sort(key=lambda x: x[joined_column])
            save_block(block_id, current_block)
            block_id += 1

    return block_id


def merge_blocks(block_number: int, joined_column: int):
    with open("./tmp/sorted.csv", 'x', newline='') as sorted_file:
        min_heap = []
        heapq.heapify(min_heap)
        open_files = []
        readers = []
        writer = csv.writer(sorted_file, delimiter=';')
        for i in range(block_number):
            file = open("./tmp/block"+str(i)+".csv", 'r', newline='')
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



if __name__ == "__main__":
    main()
