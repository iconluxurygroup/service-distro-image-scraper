import re


class LR(object):
    """docstring for LR"""

    def init(self):
        super(LR, self).init()
        self.string = None
        self.str1 = None
        self.str2 = None
        self.results = []

    def escape_ansi(self, line):
        ansi_escape = re.compile(r'(\x9B|\x1B\[)[0-?]*[ -\/]*[@-~]')
        return ansi_escape.sub('', line)

    def getAllResults(self):
        MyList = []
        intstart = 0
        strlength = len(self.string)
        continueloop = 1

        while (intstart < strlength and continueloop == 1):
            intindex1 = self.string.find(self.str1, intstart)
            if (intindex1 != -1):  # The substring was found, lets proceed
                intindex1 = intindex1 + len(self.str1)
                intindex2 = self.string.find(self.str2, intindex1)
                if (intindex2 != -1):
                    subsequence = self.string[intindex1:intindex2]
                    MyList.append(subsequence)
                    intstart = intindex2 + len(self.str2)
                else:
                    continueloop = 0
            else:
                continueloop = 0
        return MyList

    def getResult(self, string, sub1, sub2):
        string = str(string[string.find(sub1) + len(sub1):string.rfind(sub2)])
        string = string.split(sub1)[1]
        string = string.split(sub2)[0]
        return string

    def get(self, string, sub1, sub2):
        try:
            string = str(string)
            sub1 = str(sub1)
            sub2 = str(sub2)
            self.string = string
            self.str1 = sub1
            self.str2 = sub2
            if (not self.string or not self.str1 or not self.str2 or self.str1 and self.str2 not in self.string):
                return "Error: unset variables, Or substrings aren't in the main string"
        except Exception as error:
            print("Get error -> {}".format(error), end='\r\n')
            return None
        return self.getAllResults()