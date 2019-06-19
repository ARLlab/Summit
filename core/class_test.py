"""
A useful concept for blank-subtracting samples.

Any class that has an associated blank value can have the metaclass MetaClass, where properties are assigned at the
class at creation that define the property as the retrieved compound value minus a retrieved blank value for that
compound.

Blanks can be changed, and calling the corrected value will generate the true blank-subtracted value at the time it's
called, not when the blank is assigned, ie, it is ALWAYS up to date.

"""


attr_list = ['a_corrected', 'b_corrected', 'c_corrected']  # list of property names to assign


def make_getter(key):
    # Make the getters to assign as properties.
    blank_key = key.replace('_corrected', '')  # strip the key name to just the compound name without _corrected
    def getter(self):  # create function specifying the key
        # the function returns that compounds value, minus the dictionary lookup of that key, with a default of 0
        return getattr(self, blank_key) - getattr(self, 'blank').get(blank_key, 0)
    return getter


class MetaClass(type):

    def __init__(self, *args, **kwargs):
        for attr in attr_list:  # for every property name, assign a created function as the property
            setattr(self, attr, property(make_getter(attr)))
        super().__init__(self)


class Class(metaclass=MetaClass):

    def __init__(self, a, b, c, blank):
        self.a = a  # a, b, c are compound values
        self.b = b
        self.c = c
        self.blank = blank  # blank is a dict of blank values labeled by compound name


tester = Class(4, 3, 2, {'a': .25, 'b': .75, 'c': 1.5})  # create test object

print(f"Tester's values are {tester.a}, {tester.b}, {tester.c}")  # check initial values

print(f"Tester's meta values are {tester.a_corrected}, {tester.b_corrected}, {tester.c_corrected}.")