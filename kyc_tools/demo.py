


class Demo:
    def __init__(self , *args):
        self.name_list = [name.get("name") for name in args ]
        self.age_list = [age.get("age") for age in args]

    # def get_dict(self):
    #     sorted_data = dict(sorted({name: age for name, age in zip(self.name_list, self.age_list)}.items(), key=lambda value: value.get("age")))
    #     return sorted_data
    def get_dict(self):
        sorted_data = dict(sorted({name: age for name,age in zip(self.name_list,self.age_list) if age }.items(),key=lambda item: item[1]))
        return sorted_data


if __name__ == '__main__':
    demo_case = [{"name":"张三","age":28},{"name":"李四","age":20},{"name":"李四1","age":None}]
    demo_case.sort(key=lambda item:item.get["age"])
    print(demo_case)
    job = Demo(*demo_case)

    print(job.get_dict())
'''
1、前端是否传送这个参数
2、后端是否有进行非空检验
3、数据库中是否存了该参数
4、后端代码中是否引用错了不存在的参数
'''


def deom():
    dict_demo = {"age":1,"name":'123'}
    print(dict_demo["phone"])



