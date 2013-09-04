# -*-coding:utf-8 -*-

import unittest

from adapter.sqlite.cmapper import *

dbname = 'adapter/sqlite/cmapper.test.db'

class ColumnTest(unittest.TestCase):
    def test_column(self):   #
        icolumn = column('itest',CTYPE.Integer)
        self.assertEqual(100,icolumn.tvalue(100))
        self.assertTrue(icolumn.check_type(100))
        self.assertFalse(icolumn.check_type('100'))
        scolumn = column('stest',CTYPE.String)
        self.assertEqual("'a100'",scolumn.tvalue('a100'))
        self.assertTrue(scolumn.check_type('a100'))
        self.assertFalse(scolumn.check_type(100))


class MinuteConnectTest(unittest.TestCase):
    def test_put_release(self):
        xso = sobject(dbname,'test',[column('ctest1',CTYPE.Integer,'for test1'),])
        xso.open_connect()
        so = sobject(dbname,'test',[column('ctest1',CTYPE.Integer,'for test1'),])
        self.assertFalse(so.connect)
        so.put_connect(xso.connect)
        self.assertTrue(so.connect)
        so.release_connect()
        self.assertFalse(so.connect)
        xso.close_connect()

    def test_open_close(self):
        xso = sobject(dbname,'test',[column('ctest1',CTYPE.Integer,'for test1'),])
        self.assertFalse(xso.connect)
        xso.open_connect()
        self.assertTrue(xso.connect)
        xso.close_connect()
        self.assertFalse(xso.connect)

class MinuteTest(unittest.TestCase):
    def setUp(self):
        self.so = sobject(dbname,'test',[column('ctest1',CTYPE.Integer,'for test1'),
                                    column('ctest2',CTYPE.Integer,'for test2'),
                                    column('ctest3',CTYPE.String,'for test3'),
                                    column('ctest4',CTYPE.String,'for test4'),
                                    column('ctest5',CTYPE.Integer,'for test5',order_by=10),
                                    column('ctest6',CTYPE.String,'for test6',order_by=20),
                                    column('ctest7',CTYPE.String,'for test7'),
                               ],
                               primary_key = ('ctest1','ctest2'),
                               indexes = (('ctest2',),('ctest3','ctest4'),('ctest5',))
                )
        self.so.open_connect()
        '''
            drop不在tearDown中,目的是便于在测试后查看
        '''
        cursor = self.so.connect.cursor()
        cursor.execute('drop table if exists test')
        self.so.connect.commit()
        cursor.close()
 
    def tearDown(self):
        self.so.close_connect()


    ##sobject
    def test_create(self):
        self.so.create_table_if_not_exists()

    def test_crud(self):
        self.so.create_table_if_not_exists()
        bo1 = BaseObject(ctest1=101,ctest2=102,ctest3='a10',ctest4='b10',ctest5=103,ctest6='c10',ctest7='d10')
        bo2 = BaseObject(ctest1=201,ctest2=202,ctest3='a20',ctest4='b20',ctest5=103,ctest6='c20',ctest7='d20')
        bo3 = BaseObject(ctest1=301,ctest2=302,ctest3='a30',ctest4='b30',ctest5=103,ctest6='c30',ctest7='d30')
        bo4 = BaseObject(ctest1=401,ctest2=402,ctest3='a40',ctest4='b40',ctest5=403,ctest6='c40',ctest7='d40')
        bo5 = BaseObject(ctest1=501,ctest2=502,ctest3='a50',ctest4='b50',ctest5=503,ctest6='c50',ctest7='d50')
        ##insert & query
        self.so.insert([bo1,bo2,bo3,bo4,bo5])
        bos = self.so.query()
        self.assertEqual(5,len(bos))
        self.assertEqual(103,bos[0].ctest5)
        vbos1 = self.so.query_by_value(BaseObject(column=self.so.CTEST1,value=101))
        self.assertEqual(1,len(vbos1))
        vbos3 = self.so.query_by_value(BaseObject(column=self.so.CTEST5,value=103))
        self.assertEqual(3,len(vbos3))
        rbos = self.so.query_by_range(BaseObject(column=self.so.CTEST1,vfrom=101,vto=301))
        self.assertEqual(3,len(rbos))
        rbos = self.so.query_by_range(BaseObject(column=self.so.CTEST1,vfrom=101,vto=300))
        self.assertEqual(2,len(rbos))
        rbos2 = self.so.query_by_range2(BaseObject(column=self.so.CTEST1,vfrom=101,vto=401),BaseObject(column=self.so.CTEST5,vfrom=101,vto=103))
        self.assertEqual(3,len(rbos2))
        rbos = self.so.query_by_value_range(BaseObject(column=self.so.CTEST5,value=103),BaseObject(column=self.so.CTEST1,vfrom=101,vto=300))
        self.assertEqual(2,len(rbos))
        rbos = self.so.query_by_raw('ctest2>202')
        self.assertEqual(3,len(rbos))
        rbos = self.so.query_by_raw('ctest2>:ctest2',BaseObject(ctest2=200))
        self.assertEqual(4,len(rbos))
        rbos = self.so.query_by_raw('ctest3>=:ctest3',BaseObject(ctest3='a10'))
        self.assertEqual(5,len(rbos))
        #update
        #不能更新主键
        bo1.ctest1 = 1101
        bo2.ctest1 = 1201
        self.so.update([bo1,bo2])
        vbos1 = self.so.query_by_value(BaseObject(column=self.so.CTEST1,value=1101))
        self.assertEqual(0,len(vbos1))
        vbos1 = self.so.query_by_range(BaseObject(column=self.so.CTEST1,vfrom=1000,vto=2000))
        self.assertEqual(0,len(vbos1))
        bos = self.so.query()
        self.assertEqual(5,len(bos))
        self.assertEqual(101,bos[0].ctest1)
        self.assertEqual(201,bos[1].ctest1)
        #更新其它键
        bo1.ctest1 = 101
        bo2.ctest1 = 201
        bo1.ctest5 = 1103
        bo2.ctest5 = 1203
        self.so.update([bo1,bo2])
        vbos1 = self.so.query_by_value(BaseObject(column=self.so.CTEST5,value=1103))
        self.assertEqual(1,len(vbos1))
        vbos1 = self.so.query_by_range(BaseObject(column=self.so.CTEST5,vfrom=1000,vto=2000))
        self.assertEqual(2,len(vbos1))
        self.assertEqual(101,vbos1[0].ctest1)
        self.assertEqual(201,vbos1[1].ctest1)
        self.assertEqual(1103,vbos1[0].ctest5)
        self.assertEqual(1203,vbos1[1].ctest5)
        #恢复到103有3个
        bo1.ctest5 = 103
        bo2.ctest5 = 103
        self.so.update([bo1,bo2])
        vbos1 = self.so.query_by_value(BaseObject(column=self.so.CTEST5,value=103))
        self.assertEqual(3,len(vbos1))
        self.so.update_by_value(BaseObject(ctest6='1111'),BaseObject(column=self.so.CTEST5,value=103))
        vbos1 = self.so.query_by_value(BaseObject(column=self.so.CTEST6,value='1111'))
        self.assertEqual(3,len(vbos1))
        self.so.update_by_range(BaseObject(ctest3='aaaa'),BaseObject(column=self.so.CTEST2,vfrom=100,vto=300))
        vbos1 = self.so.query_by_value(BaseObject(column=self.so.CTEST3,value='aaaa'))
        self.assertEqual(2,len(vbos1))
        self.so.update_by_range2(BaseObject(ctest3='bbbb'),BaseObject(column=self.so.CTEST2,vfrom=100,vto=300),BaseObject(column=self.so.CTEST1,vfrom=100,vto=200))
        vbos1 = self.so.query_by_value(BaseObject(column=self.so.CTEST3,value='aaaa'))
        self.assertEqual(1,len(vbos1))
        self.so.update_by_value_range(BaseObject(ctest3='cccc'),BaseObject(column=self.so.CTEST2,value=100),BaseObject(column=self.so.CTEST1,vfrom=100,vto=200))
        vbos1 = self.so.query_by_value(BaseObject(column=self.so.CTEST3,value='cccc'))
        self.assertEqual(0,len(vbos1))
        self.so.update_by_value_range(BaseObject(ctest3='dddd'),BaseObject(column=self.so.CTEST2,value=202),BaseObject(column=self.so.CTEST1,vfrom=100,vto=200))
        vbos1 = self.so.query_by_value(BaseObject(column=self.so.CTEST3,value='dddd'))
        self.assertEqual(0,len(vbos1))
        self.so.update_by_value_range(BaseObject(ctest3='eeee'),BaseObject(column=self.so.CTEST2,value=202),BaseObject(column=self.so.CTEST1,vfrom=100,vto=300))
        vbos1 = self.so.query_by_value(BaseObject(column=self.so.CTEST3,value='eeee'))
        self.assertEqual(1,len(vbos1))
        self.so.update_by_raw(BaseObject(ctest6='xxxx'),'ctest1 > 201')
        vbos1 = self.so.query_by_value(BaseObject(column=self.so.CTEST6,value='xxxx'))
        self.assertEqual(3,len(vbos1))
        self.so.update_by_raw(BaseObject(ctest6='xxxx'),'ctest1 > :ctest1',BaseObject(ctest1=0))
        vbos1 = self.so.query_by_value(BaseObject(column=self.so.CTEST6,value='xxxx'))
        self.assertEqual(5,len(vbos1))
        #remove
        vbos = self.so.query()
        self.assertEqual(5,len(vbos))
        self.so.remove(vbos[:1])
        vbos = self.so.query()
        self.assertEqual(4,len(vbos))
        self.so.remove(vbos)
        vbos = self.so.query()
        self.assertEqual(0,len(vbos))
        
        bo1 = BaseObject(ctest1=101,ctest2=102,ctest3='a10',ctest4='b10',ctest5=103,ctest6='c10',ctest7='d10')
        bo2 = BaseObject(ctest1=201,ctest2=202,ctest3='a20',ctest4='b20',ctest5=103,ctest6='c20',ctest7='d20')
        bo3 = BaseObject(ctest1=301,ctest2=302,ctest3='a30',ctest4='b20',ctest5=103,ctest6='c30',ctest7='d30')
        bo4 = BaseObject(ctest1=401,ctest2=402,ctest3='a40',ctest4='b40',ctest5=403,ctest6='c40',ctest7='d40')
        bo5 = BaseObject(ctest1=501,ctest2=502,ctest3='a50',ctest4='b40',ctest5=503,ctest6='c50',ctest7='d50')
        self.so.insert([bo1,bo2,bo3,bo4,bo5])
        self.so.remove_by_value(BaseObject(column=self.so.CTEST1,value=101))
        vbos = self.so.query()
        self.assertEqual(4,len(vbos))
        self.so.remove_by_value(BaseObject(column=self.so.CTEST4,value='b20'))
        vbos = self.so.query()
        self.assertEqual(2,len(vbos))
        self.so.remove_by_value(BaseObject(column=self.so.CTEST4,value='xxx'))
        vbos = self.so.query()
        self.assertEqual(2,len(vbos))
        self.so.remove_by_range(BaseObject(column=self.so.CTEST1,vfrom=0,vto=1000))
        vbos = self.so.query()
        self.assertEqual(0,len(vbos))
    
        self.so.insert([bo1,bo2,bo3,bo4,bo5])
        self.so.remove_by_range2(BaseObject(column=self.so.CTEST1,vfrom=0,vto=1000),BaseObject(column=self.so.CTEST2,vfrom=200,vto=400))
        vbos = self.so.query()
        self.assertEqual(3,len(vbos))
        self.so.remove_by_value_range(BaseObject(column=self.so.CTEST4,value='b40'),BaseObject(column=self.so.CTEST2,vfrom=0,vto=1000))
        vbos = self.so.query()
        self.assertEqual(1,len(vbos))
        self.so.remove([bo1,bo2,bo3,bo4,bo5])
        vbos = self.so.query()
        self.assertEqual(0,len(vbos))

        self.so.insert([bo1,bo2,bo3,bo4,bo5])
        self.so.remove_by_raw('ctest1>0')
        vbos = self.so.query()
        self.assertEqual(0,len(vbos))

        self.so.insert([bo1,bo2,bo3,bo4,bo5])
        self.so.remove_by_raw('ctest1>:ctest1',BaseObject(ctest1=300))
        vbos = self.so.query()
        self.assertEqual(2,len(vbos))



if __name__ == "__main__":
    import logging
    logging.basicConfig(filename="test.log",level=logging.DEBUG,format='%(name)s:%(funcName)s:%(lineno)d:%(asctime)s %(levelname)s %(message)s')
    
    unittest.main()
