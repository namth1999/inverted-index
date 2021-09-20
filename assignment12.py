#!/usr/bin/python3
import re
import sys
from os import walk
import os.path
import multiprocessing
import string
import operator
from map_reduce_lib import *


def mapper_inverted_index(line):
    """ Map function for the inverted index exercise.
    Splits line into words, removes low information words (i.e. stopwords), scan through regular expression
    and outputs (word, file and line number).
    """
    # process_print('is processing `%s`' % line)
    output = []
    words = ''

    # Convert to lowercase, trim and remove punctuation.
    line = line.lower()

    # List with stopwords (low information words that should be removed from the string).
    STOP_WORDS = set([
        'a', 'an', 'and', 'are', 'as', 'be', 'by', 'for', 'if', 'in',
        'is', 'it', 'of', 'or', 'py', 'rst', 'that', 'the', 'to', 'with',
    ])

    data = line.split('\t')
    num = data[0]
    file = data[-1]
    for d in data:
        if d != data[0] and d != data[-1]:
            words += ' %s' % d
    words_list = words.split()
    # Split into words and add to output list.
    for word in words_list:
        #Check if word matches regular expression
        word = re.sub(r'([^\w\s]|_)+(?=\s|$)', '', word)
        # Only if word is not in the stop word list, add to output.
        if word not in STOP_WORDS:
            output.append((word, '%s@%s' % (file, num)))
    return output


def reduce_inverted_index(key_value_item):
    """ Reduce function for the inverted index exercise.
    Converts partitioned data (key, [value]) to a summary of form (key, bookmark)
    If word appears more than once, add its bookmark to a string split by ,
    """
    bookmarks = ''
    key, values = key_value_item
    for v in values:
        bookmarks += v
        if v != values[-1]:
            bookmarks += ","

    return key, bookmarks


if __name__ == '__main__':
    file_contents = []
    filenames = []
    # Parse command line arguments
    if len(sys.argv) == 1:
        print('Please provide a text-file that you want to perform the wordcount on as a command line argument.')
        sys.exit(-1)
    elif os.path.isdir(sys.argv[1]):
        filenames = next(walk(sys.argv[1]), (None, None, []))[2]
        for fn in filenames:
            with open('%s/%s' % (sys.argv[1], fn), 'r') as input_file:
                lines = input_file.read().splitlines()
                #Add the file name to ouput
                linesWithFn = [('%s\t%s' % (line, fn)) for line in lines]
                file_contents.extend(linesWithFn)
    elif os.path.isfile(sys.argv[1]):
        with open(sys.argv[1], 'r') as input_file:
            lines = input_file.read().splitlines()
            #Add the file name to output
            linesWithFn = [('%s\t%s' % (line, sys.argv[1])) for line in lines]
            file_contents.extend(linesWithFn)
    elif not os.path.isdir(sys.argv[1]) or not os.path.isfile(sys.argv[1]):
        print('File or dir `%s` not found.' % sys.argv[1])
        sys.exit(-1)

    # Execute MapReduce job in parallel.
    map_reduce = MapReduce(mapper_inverted_index, reduce_inverted_index, 8)
    bookmarks = map_reduce(file_contents, debug=True)

    print('Inverted Index:')
    for word, bookmark in bookmarks:
        print('{0}\t{1}'.format(word, bookmark))
