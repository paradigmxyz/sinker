drop schema if exists sinker cascade;
create schema sinker;

drop table if exists public.person cascade;
create table public.person
(
    id         text primary key not null,
    name       text             not null,
    created_at timestamp        not null default now()
);
insert into public.person (id, name)
values ('p-1', 'John');
insert into public.person (id, name)
values ('p-2', 'Loren');
insert into public.person (id, name)
values ('p-3', 'Prof Georgios');
insert into public.person (id, name)
values ('p-4', 'Prof Matt');

-- A student is a person
drop table if exists public.student cascade;
create table public.student
(
    id        text primary key not null,
    person_id text             not null,
    is_active boolean          not null default True,
    gpa       numeric(3, 2)    not null default 0.00,
    constraint student_person_fk
        foreign key (person_id) references public.person (id) ON DELETE CASCADE
);
insert into public.student (id, person_id, is_active, gpa)
values ('s-1', 'p-1', True, 3.01);
insert into public.student (id, person_id, is_active, gpa)
values ('s-2', 'p-2', True, 3.99);
create index student_person_id_idx on public.student (person_id);

-- A teacher is a person
drop table if exists public.teacher cascade;
create table public.teacher
(
    id        text primary key not null,
    person_id text             not null,
    salary    numeric(10, 2)   not null default 0.00,
    constraint teacher_person_fk
        foreign key (person_id) references public.person (id) ON DELETE CASCADE
);
insert into public.teacher (id, person_id, salary)
values ('t-1', 'p-3', 100000.00);
insert into public.teacher (id, person_id, salary)
values ('t-2', 'p-4', 100000.00);
create index teacher_person_id_idx on public.teacher (person_id);

-- A course is taught by a teacher
drop table if exists public.course cascade;
create table public.course
(
    id          text primary key not null,
    name        text             not null,
    description text             not null,
    teacher_id  text             not null,
    constraint course_teacher_fk
        foreign key (teacher_id) references public.teacher (id) ON DELETE CASCADE
);
insert into public.course (id, name, description, teacher_id)
values ('c-1', 'Reth', 'How to build a modern Ethereum node', 't-1');
insert into public.course (id, name, description, teacher_id)
values ('c-2', 'ZK Proofs', 'Sometimes zero knowledge is more than enough', 't-1');
insert into public.course (id, name, description, teacher_id)
values ('c-3', 'Bizchain', 'How to build a business on-chain', 't-2');
create index course_teacher_id_idx on public.course (teacher_id);

-- A student can enroll in multiple courses, and a course can have multiple students
drop table if exists public.enrollment cascade;
create table public.enrollment
(
    id         text primary key not null,
    student_id text             not null,
    course_id  text             not null,
    grade      numeric(3, 2)    not null default 0.00,
    constraint enrollment_student_fk
        foreign key (student_id) references public.student (id) ON DELETE CASCADE,
    constraint enrollment_course_fk
        foreign key (course_id) references public.course (id) ON DELETE CASCADE
);
insert into public.enrollment (id, student_id, course_id, grade)
values ('e-1', 's-1', 'c-1', 3.50);
insert into public.enrollment (id, student_id, course_id, grade)
values ('e-2', 's-2', 'c-1', 3.14);
insert into public.enrollment (id, student_id, course_id, grade)
values ('e-3', 's-1', 'c-2', 3.50);
insert into public.enrollment (id, student_id, course_id, grade)
values ('e-4', 's-2', 'c-2', 3.14);
insert into public.enrollment (id, student_id, course_id, grade)
values ('e-5', 's-1', 'c-3', 3.50);
create unique index enrollment_s_c_idx on public.enrollment (student_id, course_id);
create unique index enrollment_c_s_idx on public.enrollment (course_id, student_id);
