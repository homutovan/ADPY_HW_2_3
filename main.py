import psycopg2 as pg

params = {
    'dbname': 'homework',
    'user': 'postgres',
    'password': '1234'
        }

class DBexecutor:
    
    def __init__(self, params):
        self.params = params
        self.conn = self.connect_db()[0]
        self.curs = self.connect_db()[1]
        
    def connect_db(self):
        try:
            conn = pg.connect(**self.params)
            conn.set_session(autocommit=True)
            if conn:
                curs = conn.cursor()
                return conn, curs
        except Exception:
            print('Ошибка подключения к БД')
            return None, None


    def create_db(self):
        if self.curs:
            self.curs.execute("""CREATE TABLE student (id serial PRIMARY KEY, name varchar(100), gpa numeric(10, 2), birth timestamp with time zone);""")
            self.curs.execute("""CREATE TABLE course (id serial PRIMARY KEY, name varchar(100));""")
            self.curs.execute("""CREATE TABLE student_course (id serial PRIMARY KEY, student_id INTEGER REFERENCES student(id), course_id INTEGER REFERENCES  course(id));""")        

    def add_course(self, course_name):
        if self.curs:
            self.curs.execute("INSERT INTO course (name) VALUES (%s)", (course_name,))

    def add_student(self, student):
        if self.curs:
            self.curs.execute("INSERT INTO student (name, gpa, birth) VALUES (%s, %s, %s)", (student['name'], student['gpa'], student['birth']))
    
    def add_references(self, student_id, course_id):
        if self.curs:
            self.curs.execute("INSERT INTO student_course (student_id, course_id) VALUES (%s, %s)", (student_id, course_id))
        
    def add_students(self, course_id, students):
        if self.curs:
            last_id = self.get_last_student_id()
            self.conn.set_session(autocommit=False)
            for student in students:
                last_id += 1
                self.add_student(student)
                self.add_references(last_id, course_id)
            self.conn.commit()
            self.conn.set_session(autocommit=True)
     
    def get_students(self, course_id):
        if self.curs: 
            self.curs.execute("""SELECT s.id, s.name, c.name FROM student_course sc JOIN student s ON s.id = sc.student_id JOIN course c ON c.id = sc.course_id WHERE course_id=%s""", (course_id,))
            return self.curs.fetchall()
    
    def get_student(self, student_id):
        if self.curs:
            self.curs.execute("SELECT * FROM student WHERE id=%s", (student_id,))
            return self.curs.fetchall()
    
    def get_course(self):
        if self.curs:
            self.curs.execute("SELECT * FROM course")
            return self.curs.fetchall()
    
    def get_last_student_id(self):
        if self.curs:
            self.curs.execute("SELECT MAX(id) FROM student")
            return self.curs.fetchall()[0][0]
    
    def __del__(self):
        if self.conn:
            self.curs.close()
            self.conn.close()


student1 = {
    'name': 'Прохор Громов',
    'gpa': '4.8',
    'birth': '1954-02-23'
}

student2 = {
    'name': 'Филипп Петров',
    'gpa': '4.2',
    'birth': '1989-07-12'
}

students1 = [student1 for count in range(10)]
students2 = [student2 for count in range(10)]

if __name__ == '__main__':
    
    table1 = DBexecutor(params)

    table1.create_db() 
    table1.add_student(student1)
    table1.add_student(student2)
    print('\nВыводим имеющихся студентов по ID\n')
    print(table1.get_student(1))
    print(table1.get_student(2))
    table1.add_course('netology')
    table1.add_course('basic python')
    print('\nВыводим имеющиеся курсы\n')
    print(table1.get_course())
    table1.add_students(1, students1)
    table1.add_students(2, students2)
    print('\nВыводим студентов, зачисленных на курс с ID=1\n')
    print(table1.get_students(1))
    print('\nВыводим студентов, зачисленных на курс с ID=2\n')
    print(table1.get_students(2))
    del table1
