import json
import MySQLdb
import requests
import struct
class course_arrange_db:
    # 输入: server_url是后端服务器的地址(用于调用API), config是json格式,同MySQLdb.connect的参数
    # name       |    type           |    说明
    # host             str                  连接的数据库服务器主机名
    # user             str                  连接数据库的用户名
    # passwd            str                  连接密码
    # db               str               连接的数据库名
    def __init__(self, db_config, server_url='http://localhost'):
        self.api_root = server_url
        self.config = course_arrange_db._parseJsonStr(db_config)
        try:
            db = MySQLdb.connect(**self.config)
        except Exception as e:
            raise e

    @staticmethod
    def _parseJsonStr(x):
        if type(x) == str:
            return json.loads(x)
        return x

    @staticmethod
    def _convertResListToBitmap(resource_list: list):
        resource_list.sort()
        return 0

    @staticmethod
    def _convertBitmapToResList(resource_flag: str) -> list:
        return []

    def _findCourseInfo(self, course_id) -> list:
        db = MySQLdb.connect(**self.config)
        cur = db.cursor()
        cur.execute('SELECT * FROM CourseArrangements WHERE course_id == {}'.format(course_id))
        data = cur.fetchall()
        cur.close()
        db.close()
        return data[0] if len(data) > 0 else None

    @staticmethod
    def _convertToBitmap(x: int):
        return int(str(x), 2)

    def getPosition(self, classroom_id):
        r = self.queryClassroom(classroom_id)[0]
        return r['campus'] + ' ' + r['building'] + ' '+ r['name']

    # 输入: Json type, 可以未经反序列化, 为列表时相当于批量操作
    # 注意: 不能手动插入排课项, 只能修改
    # 参数列表:
    # name       |    type                     |    说明
    # course_id      int                            course的主键
    # time_period     unsigned int(optional)       不超过128位的正整数(每位是0/1),默认值为0(删除排课)
    # classroom_id    int(optional)                 目标更换的教室ID,无此项不移动
    def updateArrangeItem(self, params):
        params = course_arrange_db._parseJsonStr(params)
        if type(params) is not list:
            params = [params]
        count = 0
        db = MySQLdb.connect(**self.config)
        cur = db.cursor()
        for item in params:
            try:
                course_id = item['course_id']
                time_period = 0
                if 'time_period' in item.keys():
                    time_period = item['time_period']
                time_period = course_arrange_db._convertToBitmap(time_period)
                # 检查是否冲突
                course_info = self._findCourseInfo(course_id)
                if course_info is None:
                    continue
                classroom_id = course_info['classroom_id']
                teacher_id = course_info['teacher_id']
                if 'classroom_id' in item.keys():
                    classroom_id = item['classroom_id']
                if time_period & self.queryClassroomOccupiedTime(classroom_id) != 0:
                    continue
                # 教室不发生冲突, 检查教师
                if time_period & self.queryTeacherOccupiedTime(teacher_id) != 0:
                    continue
                # 都不冲突, 更新课程数据
                cur.execute("""INSERT INTO CourseArrangements VALUES ({}, {}, {}, '{}')
                                ON DUPLICATE KEY UPDATE classroom_id=VALUES({}), occupied_time=VALUES('{}');"""
                                .format(course_id, teacher_id, classroom_id,
                               time_period, classroom_id, time_period))
                db.commit()
                # TODO: 写回到基础信息管理模块
                data = {
                    'class_time': time_period,
                    'position': self.getPosition(classroom_id)
                }
                requests.post(self.api_root + '/course/modify/{}'.format(course_id), data=data)
                count += 1
            except AttributeError:
                continue
        cur.close()
        db.close()
        return count

    def queryClassroomOccupiedTime(self, classroom_id):
        db = MySQLdb.connect(**self.config)
        cur = db.cursor()
        cur.execute('SELECT occupied_time FROM CourseArrangements WHERE classroom_id={}'.format(classroom_id))
        res = cur.fetchall()
        occupied_time = 0
        for item in res:
            occupied_time |= int(item)
        cur.close()
        db.close()
        return occupied_time

    def queryTeacherOccupiedTime(self, teacher_id) -> int:
        db = MySQLdb.connect(**self.config)
        cur = db.cursor()
        cur.execute('SELECT occupied_time FROM CourseArrangements WHERE teacher_id={}'.format(teacher_id))
        res = cur.fetchall()
        occupied_time = 0
        for item in res:
            occupied_time |= int(item)
        cur.close()
        db.close()
        return occupied_time

    # 排课结束后, 调用此API将所有结果上传到数据库
    # 参数: params json字符串或python list
    # params = [item1, item2, etc.]
    # 其中每个item是数据库的一个row
    def insertArrangeData(self, params):
        params = course_arrange_db._parseJsonStr(params)
        db = MySQLdb.connect(**self.config)
        cur = db.cursor()
        for item in params:
            try:
                cur.execute("INSERT INTO CourseArrangements VALUES('{}', '{}', '{}', '{}')".format(*item))
                data = {
                    'class_time': item[3],
                    'position': self.getPosition(item[2])
                }
                requests.post(self.api_root + '/course/modify/{}'.format(item[0]), data=data)
            except Exception as e:
                continue
        db.commit()
        cur.close()
        db.close()

    def deleteClassroom(self, classroom_id) -> bool:
        db = MySQLdb.connect(**self.config)
        cur = db.cursor()
        try:
            cur.execute('DELETE FROM Classrooms WHERE id={}'.format(classroom_id))
            db.commit()
        except Exception as e:
            cur.close()
            db.close()
            return False
        cur.close()
        db.close()
        return True

    # 教室名, 校区, 教学楼
    # 输入: json or python dict
    # name          | type |   说明
    # name            str      教室名
    # campus          str      校区名
    # building        str      楼名
    # size            int      教室容量
    # resource       object    表示各类教学资源的配置情况的list,例["投影仪", "黑板", "黑板"]表示有两块黑板和一台投影仪
    def addClassroom(self, params) -> bool:
        params = course_arrange_db._parseJsonStr(params)
        db = MySQLdb.connect(**self.config)
        cur = db.cursor()
        try:
            row = [
                params['name'],
                params['building'],
                params['campus'],
                params['size'],
                _convertResListToBitmap(params['resource'])
            ]
            cur.execute('''INSERT INTO Classrooms(name, building, campus, size, resource_flag)
                            VALUES({}, "{}", "{}", "{}", "{}")'''.format(row))
            db.commit()
        except Exception as e:
            cur.close()
            db.close()
            return False
        cur.close()
        db.close()
        return True

    # 修改教室配置
    # name          | type |   说明
    # id              int      教室id
    # name            str      教室名
    # campus          str      校区名
    # building        str      楼名
    # size            int      教室容量
    # resource       object    表示各类教学资源的配置情况的list,例["投影仪", "黑板", "黑板"]表示有两块黑板和一台投影仪
    def updateClassroom(self, params) -> bool:
        params = course_arrange_db._parseJsonStr(params)
        db = MySQLdb.connect(**self.config)
        cur = db.cursor()
        try:
            row = [
                params['name'],
                params['building'],
                params['campus'],
                params['size'],
                _convertResListToBitmap(params['resource']),
                params['id']
            ]
            cur.execute('''UPDATE Classrooms SET name={}, building={}, campus={}, SIZE={}, resource_flag={}
                            WHERE id={}'''.format(row))
            db.commit()
        except Exception as e:
            cur.close()
            db.close()
            return False
        cur.close()
        db.close()
        return True

    # 查询教室数据, 当classroom_id为空时查询全部
    def queryClassroom(self, classroom_id=None) -> list:
        db = MySQLdb.connect(**self.config)
        cur = db.cursor()
        try:
            if classroom_id is None:
                cur.execute('SELECT * FROM Classrooms')
            else:
                cur.execute('SELECT * FROM Classrooms WHERE id == {}'.format(classroom_id))
            data = cur.fetchall()
            cur.close()
            db.close()
            result = []
            for item in data:
                final_item = {}
                final_item['id'] = item[0]
                final_item['name'] = item[1]
                final_item['building'] = item[2]
                final_item['campus'] = item[3]
                final_item['size'] = item[4]
                final_item['resource_flag'] = course_arrange_db._convertBitmapToResList(item[5])
                result.append(final_item)
            return result
        except Exception as e:
            raise e

    # TODO: 查询教师课表
    def queryCourseTable(self, teacher_id):
        pass
