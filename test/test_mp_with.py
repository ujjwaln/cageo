from multiprocessing.pool import Pool


class Ctx(object):

    def __init__(self, name):
        self.name = name

    def __enter__(self):
        print "%s enters" % self.name
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        print "%s exits" % self.name

    def hi(self):
        print "%s says Hi" % self.name


def f(x):
    with Ctx(x) as obj:
        obj.hi()
    #return x

if __name__ == "__main__":
    names = ['A', 'B', 'C', 'D']

    pool = Pool(processes=4)
    pool.map(f, names)

    pool.close()
    pool.join()

