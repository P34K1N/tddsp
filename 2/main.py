import os

from kedro.io import DataCatalog, MemoryDataSet
from kedro.pipeline import node, pipeline
from kedro.runner import SequentialRunner

data_catalog = DataCatalog({})

DIR_NAME = 'inputs'
OUTPUT_TEMPLATE = '{}.txt'

def distribute_func():
    words = []
    for filename in os.listdir(DIR_NAME):
        try:
            file = open('{0}/{1}'.format(DIR_NAME, filename))
            words += file.read().strip().split()
        except:
            print('Cannot open file ', filename)
    l = len(words) // 4
    return words[:l], words[l:2*l], words[2*l:3*l], words[3*l:]


def map_func(words):
    counts = {'a': {}, 'b': {}, 'c': {}, 'd': {}}
    for word in words:
        k = word[0]
        if k in {'a', 'b', 'c', 'd'}:
            if word in counts[k]:
                counts[k][word] += 1
            else:
                counts[k][word] = 1
    return counts


def get_groupby_func(letters):
    def groupby_func(counts1, counts2, count3, counts4):
        counts = {}
        for l in letters:
            counts[l] = {}
            for count in [counts1, counts2, count3, counts4]:
                for word in count[l]:
                    if word in counts[l]:
                        counts[l][word] += count[l][word]
                    else:
                        counts[l][word] = count[l][word]
        return counts

    return groupby_func

def get_reduce_func(letters, output_file):
    def reduce_func(counts):
        file = open(output_file, 'w')
        for l in letters:
            max_word = ''
            max_count = 0
            for word, count in counts[l].items():
                if max_count < count:
                    max_count = count
                    max_word = word
            print(max_word, max_count, file=file)

    return reduce_func

greeting_pipeline = pipeline(
    [
        node(
            distribute_func,
            inputs=None,
            outputs=['words1', 'words2', 'words3', 'words4'],
        ),
    ] + [
        node(
            map_func,
            inputs='words{}'.format(i),
            outputs='counts{}'.format(i),
        ) for i in [1, 2, 3, 4]
    ] + [
        node(
            get_groupby_func(letters),
            inputs=['counts1', 'counts2', 'counts3', 'counts4'],
            outputs='counts_{}'.format(''.join(letters)),
        ) for letters in [['a', 'b'], ['c', 'd']]
    ] + [
        node(
            get_reduce_func(letters, OUTPUT_TEMPLATE.format(''.join(letters))),
            inputs='counts_{}'.format(''.join(letters)),
            outputs=None,
        ) for letters in [['a', 'b'], ['c', 'd']]
    ]
)

runner = SequentialRunner()

runner.run(greeting_pipeline, data_catalog)
