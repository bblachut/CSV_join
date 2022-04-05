import sys
import csv
from typing import List

"""Buffer size determines how many characters we can handle at one time """


def main():
    # file_path_left = sys.argv[2]
    # file_path_right = sys.argv[3]
    # column_name = sys.argv[4]
    # join_type = sys.argv[5]

    file_path_left = "left.csv"
    column_name = "Surname"

    joined_column_left = get_joined_column_id(file_path_left, column_name)
    # joined_column_right = get_joined_column_id(file_path_right, column_name)

    external_sort(10, file_path_left, joined_column_left)


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
    with open(file_path, 'w', newline='') as file:
        writer = csv.writer(file,  delimiter=';')
        writer.writerows(block)


########### sorting csv files ###########
def external_sort(buffer_size: int, file_path: str, joined_column: int):
    save_sorted_blocks(buffer_size, file_path, joined_column)
    merge_blocks()


def save_sorted_blocks(buffer_size: int, file_path: str, joined_column: int) -> int:
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
            if characters > buffer_size:
                current_block.sort(key=lambda x: x[joined_column])
                save_block(block_id, current_block)
                block_id += 1
                current_block = []
        if characters > 0:
            current_block.sort(key=lambda x: x[joined_column])
            save_block(block_id, current_block)

    return block_id


def merge_blocks():
    pass


if __name__ == "__main__":
    main()
