from name_yalsooni.crawler_py.ebest.xaquery.command.abstract import Command


class T8430Command(Command):

    TR_NAME = "t8430"

    def __init__(self):
        super(T8430Command, self).__init__(self.TR_NAME)
