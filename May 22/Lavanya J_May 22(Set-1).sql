MongoDB Schema & Relationships Exercise Set 
Section 1: Working with Schemas & Data Types 

1.Create a database named trainingdb

use trainingdb

Output:
switched to db trainingdb

2.Create a collection employees with documents that include: 
name (string) 
age (number) 
isManager (boolean) 
skills (array of strings) 
joiningDate (date) 
profile (sub-document with linkedin and portfolio ),
3.Insert 4 employees with varying skill sets and joining dates. 

db.employees.insertMany([
  {
    name: "Amal",
    age: 28,
    isManager: false,
    skills: ["JavaScript", "MongoDB"],
    joiningDate: new Date("2022-03-01"),
    profile: {
      linkedin: "https://linkedin.com/in/amal",
      portfolio: "https://amal.dev"
    }
  },
  {
    name: "Babu",
    age: 35,
    isManager: true,
    skills: ["Java", "Spring", "MongoDB", "Docker"],
    joiningDate: new Date("2021-07-10"),
    profile: {
      linkedin: "https://linkedin.com/in/babu",
      portfolio: "https://babu.dev"
    }
  },
  {
    name: "Cary",
    age: 25,
    isManager: false,
    skills: ["Python", "Flask", "SQL"],
    joiningDate: new Date("2023-01-15"),
    profile: {
      linkedin: "https://linkedin.com/in/cary",
      portfolio: "https://cary.dev"
    }
  },
  {
    name: "Dev",
    age: 40,
    isManager: true,
    skills: ["C#", ".NET", "Azure", "DevOps"],
    joiningDate: new Date("2022-09-20"),
    profile: {
      linkedin: "https://linkedin.com/in/dev",
      portfolio: "https://dev.dev"
    }
  }
]);

Output:
{
  acknowledged: true,
  insertedIds: {
    '0': ObjectId('683032be130c0e3e20c200e8'),
    '1': ObjectId('683032be130c0e3e20c200e9'),
    '2': ObjectId('683032be130c0e3e20c200ea'),
    '3': ObjectId('683032be130c0e3e20c200eb')
  }
}
 
4.Query all employees who: 
Have more than 2 skills 
Joined after a specific date 

db.employees.find({
  $and: [
    { "skills.2": { $exists: true } },
    { joiningDate: { $gt: new Date("2022-01-01") } }
  ]
});

Output:
{
  _id: ObjectId('683032be130c0e3e20c200ea'),
  name: 'Cary',
  age: 25,
  isManager: false,
  skills: [
    'Python',
    'Flask',
    'SQL'
  ],
  joiningDate: 2023-01-15T00:00:00.000Z,
  profile: {
    linkedin: 'https://linkedin.com/in/cary',
    portfolio: 'https://cary.dev'
  }
}
{
  _id: ObjectId('683032be130c0e3e20c200eb'),
  name: 'Dev',
  age: 40,
  isManager: true,
  skills: [
    'C#',
    '.NET',
    'Azure',
    'DevOps'
  ],
  joiningDate: 2022-09-20T00:00:00.000Z,
  profile: {
    linkedin: 'https://linkedin.com/in/dev',
    portfolio: 'https://dev.dev'
  }
}

5.Add a new field rating (float) to one employee

db.employees.updateOne({ name: "Babu" }, { $set: { rating: 4.5 } });

Output:
{
  acknowledged: true,
  insertedId: null,
  matchedCount: 1,
  modifiedCount: 1,
  upsertedCount: 0
}

6.Find all employees with rating field of type double

db.employees.find({ rating: { $type: "double" } });

Output:
{
  _id: ObjectId('683032be130c0e3e20c200e9'),
  name: 'Babu',
  age: 35,
  isManager: true,
  skills: [
    'Java',
    'Spring',
    'MongoDB',
    'Docker'
  ],
  joiningDate: 2021-07-10T00:00:00.000Z,
  profile: {
    linkedin: 'https://linkedin.com/in/babu',
    portfolio: 'https://babu.dev'
  },
  rating: 4.5
}

7.Exclude the _id field in a query result and show only name and skills 

db.employees.find({}, { _id: 0, name: 1, skills: 1 });

Output:
{
  name: 'Amal',
  skills: [
    'JavaScript',
    'MongoDB'
  ]
}
{
  name: 'Babu',
  skills: [
    'Java',
    'Spring',
    'MongoDB',
    'Docker'
  ]
}
{
  name: 'Cary',
  skills: [
    'Python',
    'Flask',
    'SQL'
  ]
}
{
  name: 'Dev',
  skills: [
    'C#',
    '.NET',
    'Azure',
    'DevOps'
  ]
}

Section 2: One-to-One (Embedded) 
1.Create a database schooldb

use schooldb

Output:
switched to db schooldb

2.In the students collection, insert 3 student documents with: 
Embedded guardian sub-document ( name , phone , relation )

db.students.insertMany([
  {
    name: "Eve",
    guardian: {
      name: "Anna",
      phone: "1234567890",
      relation: "Mother"
    }
  },
  {
    name: "Frank",
    guardian: {
      name: "John",
      phone: "0987654321",
      relation: "Father"
    }
  },
  {
    name: "Grace",
    guardian: {
      name: "Lily",
      phone: "1122334455",
      relation: "Mother"
    }
  }
]);

Output:
{
  acknowledged: true,
  insertedIds: {
    '0': ObjectId('68303614f4b9c4ed100a95b3'),
    '1': ObjectId('68303614f4b9c4ed100a95b4'),
    '2': ObjectId('68303614f4b9c4ed100a95b5')
  }
}

3.Query students where the guardian is their "Mother" 

db.students.find({ "guardian.relation": "Mother" });

Output:
{
  _id: ObjectId('68303614f4b9c4ed100a95b3'),
  name: 'Eve',
  guardian: {
    name: 'Anna',
    phone: '1234567890',
    relation: 'Mother'
  }
}
{
  _id: ObjectId('68303614f4b9c4ed100a95b5'),
  name: 'Grace',
  guardian: {
    name: 'Lily',
    phone: '1122334455',
    relation: 'Mother'
  }
}

4.Update the guardian's phone number for a specific student

db.students.updateOne(
  { name: "Grace" },
  { $set: { "guardian.phone": "6677889900" } }
);

Output:
{
  acknowledged: true,
  insertedId: null,
  matchedCount: 1,
  modifiedCount: 1,
  upsertedCount: 0
}

Section 3: One-to-Many (Embedded) 
1.In the same schooldb , create a teachers collection, 
2.Insert documents where each teacher has an embedded array of classes they teach (e.g., ["Math", "Physics"] ) 

use schooldb
switched to db schooldb

db.teachers.insertMany([
  { name: "Mr. Smith", classes: ["Math", "Physics"] },
  { name: "Ms. Johnson", classes: ["Biology", "Chemistry"] },
  { name: "Dr. Lee", classes: ["Physics", "Computer Science"] }
]);

Output:
{
  acknowledged: true,
  insertedIds: {
    '0': ObjectId('68305d3adb2af2d250f2c3b4'),
    '1': ObjectId('68305d3adb2af2d250f2c3b5'),
    '2': ObjectId('68305d3adb2af2d250f2c3b6')
  }
}

3.Query teachers who teach "Physics" 

db.teachers.find({ classes: "Physics" });

Output:
{
  _id: ObjectId('68305d3adb2af2d250f2c3b4'),
  name: 'Mr. Smith',
  classes: [
    'Math',
    'Physics'
  ]
}
{
  _id: ObjectId('68305d3adb2af2d250f2c3b6'),
  name: 'Dr. Lee',
  classes: [
    'Physics',
    'Computer Science'
  ]
}

4.Add a new class "Robotics" to a specific teacher's classes array

db.teachers.updateOne(
  { name: "Dr. Lee" },
  { $addToSet: { classes: "Robotics" } }
);

Output:
{
  acknowledged: true,
  insertedId: null,
  matchedCount: 1,
  modifiedCount: 1,
  upsertedCount: 0
}

5.Remove "Math" from one teacher’s class list

db.teachers.updateOne(
  { name: "Mr. Smith" },
  { $pull: { classes: "Math" } }
);

Output:
{
  acknowledged: true,
  insertedId: null,
  matchedCount: 1,
  modifiedCount: 1,
  upsertedCount: 0
}

Section 4: One-to-Many (Referenced) 
1.Create a database academia,
2.Insert documents into courses with fields: 
_id 
title 
credits 

use academia
switched to db academia

db.courses.insertMany([
  { _id: ObjectId("60c72b2f9af1f2d3c0e5f001"), title: "Machine Learning", credits: 4 },
  { _id: ObjectId("60c72b2f9af1f2d3c0e5f002"), title: "Algorithms", credits: 3 }
]);

Output:
{
  acknowledged: true,
  insertedIds: {
    '0': ObjectId('60c72b2f9af1f2d3c0e5f001'),
    '1': ObjectId('60c72b2f9af1f2d3c0e5f002')
  }
}

3.Insert documents into students with fields:
name 
enrolledCourse (store ObjectId reference to a course) 

db.students.insertMany([
  { name: "Lily", enrolledCourse: ObjectId("60c72b2f9af1f2d3c0e5f001") },
  { name: "Jade", enrolledCourse: ObjectId("60c72b2f9af1f2d3c0e5f002") }
]);

Output:
{
  acknowledged: true,
  insertedIds: {
    '0': ObjectId('68305e405180b154d126c4e9'),
    '1': ObjectId('68305e405180b154d126c4ea')
  }
}

4.Query students who are enrolled in a specific course (by ObjectId )

db.students.find({ enrolledCourse: ObjectId("60c72b2f9af1f2d3c0e5f001") });

Output:
{
  _id: ObjectId('68305e405180b154d126c4e9'),
  name: 'Lily',
  enrolledCourse: ObjectId('60c72b2f9af1f2d3c0e5f001')
}

5.Query the course details separately using the referenced _id 

db.courses.find({ _id: ObjectId("60c72b2f9af1f2d3c0e5f001") });

Output:
{
  _id: ObjectId('60c72b2f9af1f2d3c0e5f001'),
  title: 'Machine Learning',
  credits: 4
}

Section 5: $lookup (Join in Aggregation) 
1.Use the academia database,
2.Use $lookup to join students with courses based on enrolledCourse,
3.Show only student name , and course title in the output using $project 
 
db.students.aggregate([
  {
    $lookup: {
      from: "courses",
      localField: "enrolledCourse",
      foreignField: "_id",
      as: "courseDetails"
    }
  },
  {
    $unwind: "$courseDetails"
  },
  {
    $project: {
      _id: 0,
      name: 1,
      course: "$courseDetails.title"
    }
  }
]);

Output:
{
  name: 'Lily',
  course: 'Machine Learning'
}
{
  name: 'Jade',
  course: 'Algorithms'
}

4.Add a $match after $lookup to get only students enrolled in "Machine Learning" course

db.students.aggregate([
  {
    $lookup: {
      from: "courses",
      localField: "enrolledCourse",
      foreignField: "_id",
      as: "courseDetails"
    }
  },
  { $unwind: "$courseDetails" },
  { $match: { "courseDetails.title": "Machine Learning" } },
  {
    $project: {
      _id: 0,
      name: 1,
      course: "$courseDetails.title"
    }
  }
]);

Output:
{
  name: 'Lily',
  course: 'Machine Learning'
}





 



