#-*- coding: utf-8 -*-

import unittest

from utils import *

class ModuleTest(unittest.TestCase):
    def test_merge(self):
        a = [1,3,5,7]
        b = [2,4,6,8,10]
        self.assertEqual([1,2,3,4,5,6,7,8,10],merge(a,b))
        self.assertEqual([1,3,5,7],merge([],a))
        self.assertEqual([1,3,5,7],merge(a,[]))
        self.assertEqual([],merge([],[]))

    def test_merge_m(self):
        a = [10,30,31,70]
        b = [20,40,60,80,100]
        c = [11,31,51,71]
        d = [21,41,61,81]
        e = [12,32,42,52]
        f = [22,42,62,82,102,103]
        self.assertEqual([10,11,20,30,31,31,40,51,60,70,71,80,100],merge_m(a,b,c))
        self.assertEqual([10,11,12,20,21,22,30,31,31,32,40,41,42,42,51,52,60,61,62,70,71,80,81,82,100,102,103],merge_m(a,b,c,d,e,f))


if __name__ == "__main__":
    import logging
    logging.basicConfig(filename="test.log",level=logging.DEBUG,format='%(name)s:%(funcName)s:%(lineno)d:%(asctime)s %(levelname)s %(message)s')
    
    unittest.main()    
