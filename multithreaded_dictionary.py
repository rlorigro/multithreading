import multiprocessing
from multiprocessing import Pool

from multithreading.FileManager import FileManager


def parallel_run(a, b, file_path, return_dict):
    """
    Run this method in parallel
    :param a: arbitrary parameter a
    :param b: arbitrary parameter b
    :return:
    """
    data = None    # do something with the file

    key = file_path               # unique key identifying this thread (i.e. chromosome number, position, w/e)

    print(a, b, file_path, len(return_dict.keys()))

    return_dict[key] = data


def parallelization(a, b, directory_of_files, max_threads, output_dir_path):
    """
    This method generates a list of parameters to be distributed to functions in separate threads, and stores their
    outputs in a common dictionary
    :param a: user defined parameter
    :param b: user defined parameter
    :param directory_of_files: a directory containing files that need to be parsed in parallel
    :return:
    """
    manager = multiprocessing.Manager()
    return_dict = manager.dict()

    # get entire set of files
    file_manager = FileManager()
    file_paths = file_manager.get_file_paths_from_directory(directory_of_files)

    args = list()

    if len(file_paths) < max_threads:
        max_threads = len(file_paths)

    # generate exhaustive list of arguments to be sent to each thread
    for file_path in file_paths:
        args.append((a, b, file_path, return_dict))

    # initiate threading
    with Pool(processes=max_threads) as pool:
        pool.starmap(parallel_run, args)

    for key in return_dict.keys():
        print(return_dict[key])

    # write to output_dir


# a and b are placeholders for some other parameter that parallel_run() may also need to do its work
parallelization(a=1, b=2, directory_of_files=".", max_threads=5, output_dir_path=".")


