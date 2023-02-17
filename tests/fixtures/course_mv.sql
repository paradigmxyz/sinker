select id,
       json_build_object('name', "name", 'description', "description", 'teacher',
                         (select json_build_object('salary', "salary", 'person',
                                                   (select json_build_object('name', "name")
                                                    from person
                                                    where person.id = person_id))
                          from teacher
                          where teacher.id = teacher_id), 'enrollments',
                         (select json_agg(json_build_object('grade', "grade",
                                                            'student', (select json_build_object(
                                                                                   'gpa', "gpa",
                                                                                   'person',
                                                                                   (select json_build_object(
                                                                                               'name',
                                                                                               "name"
                                                                                               )
                                                                                    from person
                                                                                    where person.id = person_id)
                                                                                   )
                                                                        from student
                                                                        where student.id = student_id)
                             ))
                          from enrollment
                          where enrollment.course_id = course.id)
           ) as "course"
from "course";
