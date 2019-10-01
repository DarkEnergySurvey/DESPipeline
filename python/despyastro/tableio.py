#!/usr//bin/env python
"""
    .. _despyastro-tableio:

    **tableio**
    -----------

    A collection of scripts to read and write very simple ascii tables.

"""

import string
import numpy

#Read/write headers

def get_header(fname):
    """
        Returns a string containing all the lines at the top of a file which start with '#'
    """
    buff = ''
    for line in open(fname).readlines():
        if line.startswith('#'):
            buff += line
        else:
            break
    return buff

def put_header(fname, text):
    """ Adds text (starting by # and ending with a new line) to the top of a file.

        Parameters
        ----------
        fname : str
            Filename

        text : str
            Text to add

    """
    if not text:
        return
    if not text.startswith('#'):
        text = '#' + text
    if not text.endswith('\n'):
        text += '\n'
    buff = text + open(file).read()
    open(fname, 'w').write(buff)

#Files containing strings

def get_str(fname, cols=0, nrows='all', sep=None):
    """
        Reads strings from a file
        Examples
        --------
	    >>>  x, y, z = get_str('myfile.cat', (0, 1, 2))
        >>>  x, y, z are returned as string lists
    """
    # Modified to be feed a buffer as well as a file
    # F. Menanteau
    if isinstance(cols, int):
        cols = (cols,)
        nvar = 1
    else:
        nvar = len(cols)
    lista = [[]] * nvar

    if isinstance(fname, list):
        buff = fname
        #print "# Passing a buffer"
    else:
        buff = open(fname).readlines()

    if nrows == 'all':
        nrows = len(buff)
        counter = 0
    for lines in buff:
        if counter >= nrows:
            break
        if sep:
            pieces = string.split(lines, sep)
        else:
            pieces = string.split(lines)

        if not pieces:
            continue
        if pieces[0].startswith('#'):
            continue
        for j in range(nvar):
            lista[j].append(pieces[cols[j]])

        counter += 1
    if nvar == 1:
        return lista[0]
    return tuple(lista)

def put_str(fname, tupla):
    """ Writes tuple of string lists to a file
        Examples
        --------
	    >>>  put_str(file,(x,y,z))
    """
    if not isinstance(tupla, tuple):
        raise Exception('Need a tuple of variables')

    f = open(fname, 'w')

    for i in range(1, len(tupla)):
        if len(tupla[i]) != len(tupla[0]):
            raise Exception('Variable lists have different length')
    for i in range(len(tupla[0])):
        cosas = []
        for j in range(len(tupla)):
            cosas.append(str(tupla[j][i]))
        f.write(cosas + '\n')
    f.close()


#Files containing data

def get_data(fname, cols=0, nrows='all', sep=None):
    """ Returns data in the columns defined by the tuple
        (or single integer) cols as a tuple of float arrays
        (or a single float array)
    """
    if isinstance(cols, int):
        cols = (cols,)
        nvar = 1
    else:
        nvar = len(cols)

    data = get_str(fname, cols, nrows, sep=sep)

    if nvar == 1:
        return numpy.array(map(float, data))

    data = list(data)
    for j in range(nvar):
        data[j] = numpy.array(map(float, data[j]))
    return tuple(data)

def put_data(fname, variables, header='', fmt='', append='no'):
    """ Writes tuple of float variables to a file
        Examples
        --------
	    >>>  put_data(file,(x,y,z),header,format)

	    where header is any string
        and format is a string of the type:
            '%f %f %i '
    """
    if isinstance(variables, tuple):
        raise Exception('Need a tuple of variables')
    if not fmt:
        fmt = '%.8e  ' * len(variables)
    if append == 'yes':
        f = open(fname, 'a')
    else:
        f = open(fname, 'w')
    if header:
        if not header.startswith('#'):
            header = '#' + header
        if not header.endswith('\n'):
            header += '\n'
        f.write(header)
    for i in range(len(variables[0])):
        cosas = []
        for j in range(len(variables)):
            cosas.append(variables[j][i])
        line = fmt % tuple(cosas)
        f.write(line + '\n')
    f.close()


#Files containing strings

# F. Menanteau, reads all or some columns

def rcols(fname, cols=None, nrows='all'):
    """ Returns data in the columns defined by the tuple
        (or single integer) cols as a tuple of float arrays
        (or a single float array)
    """

    if cols is None:
        nvar = 0
    elif isinstance(cols, int):
        cols = (cols,)
        nvar = 1
    else:
        nvar = len(cols)

    data = get_string(fname, cols, nrows)

    if nvar == 1:
        return numpy.array(map(float, data))

    data = list(data)
    nvar = len(data)
    for j in range(nvar):
        data[j] = numpy.array(map(float, data[j]))
    return tuple(data)

def get_string(fname, cols=None, nrows='all', buff=None):
    """
        Reads strings from a file
        Examples
        --------
	    >>>  x,y,z=get_str('myfile.cat',(0,1,2))
        >>>  x,y,z are returned as string lists

        Modified to read from buffer, F. Menanteau

    """
    nvar = None

    if buff:
        buff = fname
    else:
        buff = open(fname).readlines()
    if nrows == 'all':
        nrows = len(buff)
    counter = 0
    for lines in buff:
        if counter >= nrows:
            break
        pieces = string.split(lines)
        if not pieces:
            continue
        if pieces[0].startswith('#'):
            continue

        # Decide how many columns to read
        if nvar is None:

            if cols is None:
                nvar = len(pieces)
                cols = tuple(range(nvar))
            elif isinstance(cols, int):
                cols = (cols,)
                nvar = 1
            else:
                nvar = len(cols)
            lista = [[]] * nvar

        for j in range(nvar):
            lista[j].append(pieces[cols[j]])
        counter += 1

    if nvar == 1:
        return lista[0]
    return tuple(lista)


def get_datarray(fname, cols=0, nrows='all', buff=None):
    """ Returns data in the columns defined by the tuple
        (or single integer) cols as a tuple of float arrays
        (or a single float array)

        Modified to read from buffer, F. Menanteau
    """

    if isinstance(cols, 0):
        cols = (cols,)
        nvar = 1
    else:
        nvar = len(cols)

    data = get_string(fname, cols, nrows, buff)

    if nvar == 1:
        return numpy.array(map(float, data))

    data = list(data)
    for j in range(nvar):
        data[j] = numpy.array(map(float, data[j]))
    return tuple(data)


#Read/write 2D arrays
# Added from useful.py

def get_2Darray(fname, cols='all', nrows='all', verbose='no'):
    """ Read the data on the defined columns of a file
        to an 2 array

        Parameters
        ----------
        fname : str
            File name

        cols : str
            Something

        nrows : str
            Something

        verbose : str
            Something

        Examples
        --------
        >>>  x = get_2Darray(file)
        >>>  x = get_2Darray(file,range(len(p))
        >>>  x = get_2Darray(file,range(0,10,2),nrows=5000)

        Returns
        -------
        x(nrows,ncols)
    """
    if cols == 'all':
        #Get the number of columns in the file
        for line in open(fname).readlines():
            pieces = string.split(line)
            if not pieces:
                continue
            if line.startswith('#'):
                continue
            nc = len(pieces)
            cols = range(nc)
            if verbose == 'yes':
                print 'cols=', cols
            break
    else:
        nc = len(cols)

    lista = get_data(file, cols, nrows)
    nl = len(lista[0])
    x = numpy.zeros((nl, nc), type='Float64') # Float64 to avoid warning msgs.
    for i in range(nc):
        x[:, i] = lista[i]

    return x

def put_2Darray(fname, array, header='', fmt='', append='no'):
    """ Writes a 2D array to a file, where the first
        index changes along the lines and the second along
        the columns
        Examples
        --------
        >>>  put_2Darray(file,a,header,format)

        where header is any string
        and format is a string of the type:
        '%f %f %i '
    """
    lista = []
    for i in range(array.shape[1]):
        lista.append(array[:, i])
    lista = tuple(lista)
    put_data(fname, lista, header, fmt, append)
