import functools
import pandas as pd

def matrix_op(func):
    '''
    Convert data to matrix for before running the function
    '''

    @functools.wraps(func)
    def wrapper_decorator(*args, **kwargs):
        args = list(args)
        for i in range(len(args)):
            if args[i].shape[1] != 1:
                raise Exception('matrix_op only accept Pandas DataFrame with 1 column')
            args[i] = transpose(args[i])

        args = tuple(args)
        result = func(*args, **kwargs)
        result = invtranspose(result)
        return result

    return wrapper_decorator


def transpose(data):
    temp = data.copy()
    temp = temp.reset_index(['date', 'stock'])
    result = temp.pivot_table(columns='stock', 
                              index='date', 
                              values=temp.columns[0],
                              aggfunc='last')
    return result


def invtranspose(dataT):
    dataT = dataT.reset_index().melt(id_vars='date')
    dataT = dataT.set_index(['date', 'stock'])
    return dataT