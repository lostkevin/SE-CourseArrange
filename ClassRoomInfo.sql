CREATE DATABASE `RESOURCEMANAGER`;
create TABLE `Classrooms` (
	id int primary key auto_increment,
    name CHAR(60) unique not null,
    size int default 0,
	resource_flag CHAR(50) NOTNULL DEFAULT(0),
);

create table `CourseArrangements` (
	course_id int primary key,
    teacher_id int,
    classroom_id int,
    occupied_time CHAR(50) NOTNULL DEFAULT(0),
    foreign key(classroom_id) references Classrooms(ID)
);
