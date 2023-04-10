import bonobo

class extract():

    def generate_data(self):
        yield 'foo'
        yield 'bar'
        yield 'baz'

def uppercase(x: str):
    return x.upper()

def output(x: str):
    print(x)


if __name__ == '__main__':

    graph = bonobo.Graph(
        extract.generate_data(),
        uppercase,
        output,
    )
    bonobo.run(graph)
