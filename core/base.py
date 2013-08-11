#-*- coding: utf-8 -*-

from inspect import (
            getargspec,
        )
import lib.decorator as decorator


#####BaseObject
class BaseObject(object):
    def __init__(self,**kwargs):
        self.__dict__.update(kwargs)

    #has_attr/get_attr/set_attr没有必要, 系统函数hasattr/getattr/setattr已实现这些功能
    def has_attr(self,attr_name):
        return attr_name in self.__dict__

    def get_attr(self,attr_name):
        #print('BaseObject,get_attr %s' % (attr_name,))
        return self.__dict__[attr_name]

    def set_attr(self,attr_name,value):
        self.__dict__[attr_name] = value

    def __repr__(self):
        return 'BaseObject'

    def mydict(self):
        return self.__dict__

    def __len__(self):
        return len(self.__dict__)

class CommonObject(BaseObject):
    def __init__(self,id,**kwargs):
        BaseObject.__init__(self,**kwargs)
        self.id = id

    def __repr__(self):
        return 'CommonObject'


###indicator
#快速键值
'''
    因为这里使用了id函数，所以必须妥善处理临时的输入序列参数
        此时，某个序列对象被回收，然后会重新分配给另一个序列对象，
        而这个对象又用于同一个被indicator修饰的函数,最终导致紊乱
'''
def quick_id(v):
    '''
        对基础数据类型，返回(v,None)
        对对象类型，返回(id(v),v)
    '''
    t = type(v)
    if t == list: #用到最多
        return id(v),v
    elif t  == int or t == float: #次多
        return v,None
    #elif t in [long,bool,complex,str,unicode,xrange]:
    elif t in [bool,complex,str,range]:    #python3不再有long,unicode(默认str即为unicode)
        return v,None
    #其余都为对象，用id标识
    return id(v),v

def quick_ids(vs):
    idv =  [quick_id(v) for v in vs]
    return tuple((i for i,v in idv)),[v for i,v in idv if v!=None]


import gc
class ObjHolder(object):
    '''
        用于保持对象引用，避免释放、回收后被重新分配       
    '''
    def __init__(self):
        self.holder = {}

    def register_obj(self,obj):
        if id(obj) not in self.holder:
            self.holder[id(obj)] = obj
        assert self.holder[id(obj)] is obj

    def register_objs(self,objs):
        for obj in objs:
            self.register_obj(obj)

    def reset(self):
        self.holder.clear()
        gc.collect()

#用于持有住各indicator的输入对象，避免临时对象被回收后重新分配给用于同一indicator的序列对象，从而导致隐秘错误
GLOBAL_HOLDER = ObjHolder()


def _indicator(func, *args, **kw):
    '''
        indicator装饰器,用于常规indicator的实现
        向原函数提供暂存对象, 要求原函数的最后一个位置参数必须是_ts
        vojb用于固定住输入中用id标识的对象，防止在计算过程中被释放后重新分配给其它对象
    '''
    #print 'in _indicator'
    vargs = list(args)
    key = vargs + kw.values() if kw else vargs
    #print vargs
    vkey,vobjs = quick_ids(key)    
    storage = func.storage
    if vkey not in storage:
        #storage[vkey] = BaseObject()
        storage[vkey] = BaseObject(initialized = False) #_ts
        GLOBAL_HOLDER.register_objs(vobjs)
    #print vargs
    #指标调用者直接指定_ts(用位置或命名参数)时，仍然将其替换为暂存者. 要求调用者不得指定这个参数，否则会导致莫名奇妙问题
    #vargs[-1] = storage[vkey]
    if vargs[-1] == None:  #最后一个位置参数为None,就认为是_ts；这里会略为出现紊乱，如出现刻意为None的其它参数
        vargs[func.tpos] = storage[vkey]    #允许_ts不在最后位置，以支持可变参数*args
    else:   #后面有可变位置参数, 此时,_ts被可变参数填充, 实际总长度比应当总长度少1，应将_ts插入到该位置
        vargs = vargs[:func.tpos] + [storage[vkey]] + vargs[func.tpos:]
        #print 'vargs=',vargs
    return func(*vargs,**kw)

def indicator(f):
    f.storage = {}
    aspecs = getargspec(f).args
    f.tpos = aspecs.index('_ts')
    assert f.tpos == len(aspecs)-1,'position of _ts is invalid'  #_ts必须是最后一个固定位置参数
    return decorator.decorator(_indicator,f)


##############
# 一个例子
#     可用的ma  
##############
@indicator
def MA_EXAMPLE(src,mlen,_ts=None):
    '''
        所有指标都必须设定_ts这个参数,且默认值为None,装饰器将传入暂存对象
        #_ts必须是最后一个固定位置参数

        返回值:
            移动平均序列
            当序列中元素个数<mlen时，结果序列为到该元素为止的所有元素值的平均
    '''
    assert mlen>0, 'mlen should > 0'
    if not _ts.initialized:
        _ts.initialized = True
        _ts.sa = [0]*mlen   #哨兵
        _ts.ma = []

    slen = len(_ts.ma)
    ss = _ts.sa[-1]
    for i in range(slen,len(src)):
        ss += src[i]
        _ts.sa.append(ss)
        #print ss,_ts.sa[i-mlen]
        #当累计个数<nlen时，求其平均值，而不是累计值/mlen
        rlen = mlen if mlen < i+1 else i+1
        _ts.ma.append((ss-_ts.sa[-rlen-1]+rlen/2)/rlen) 
    #print _ts.sa
    return _ts.ma

#######
#基本数据结构
#######
class TICK(object):
    __slots__ = [
                 'cname',
                 'date',
                 'min1',
                 'sec',
                 'msec',
                 'time',
                 'holding',
                 'dvolume',
                 'price',
                 'high',
                 'low',
                 'bid_price',
                 'bid_volume',
                 'ask_price',
                 'ask_volume',
                 'switch_min',
                 'iorder',
                 'dorder',
                ]


ITYPE_UNKNOWN = 0
ITYPE_L2H = 1
ITYPE_H2L = 2

class XMIN(object):
    __slots__ = [
                 'cname',
                 'idate',
                 'imin',
                 'iopen',
                 'iclose',
                 'ihigh',
                 'ilow',
                 'ivolume',
                 'iholding',
                 'itype',
                ]
    def __init__(self,cname,idate,imin,iopen,iclose,ihigh,ilow,ivolume,iholding,itype=0):
        self.cname = cname
        self.idate = idate
        self.imin = imin
        self.iopen = iopen
        self.iclose = iclose
        self.ihigh = ihigh
        self.ilow = ilow
        self.ivolume = ivolume
        self.iholding = iholding
        self.itype = itype

    def __repr__(self):
        return '%s:%d-%d %d-%d-%d-%d %d-%d %d' % (self.cname,self.idate,self.imin,self.iopen,self.iclose,self.ihigh,self.ilow,self.ivolume,self.iholding,self.itype)
