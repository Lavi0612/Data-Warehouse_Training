MongoDB Exercise Set #2: Project & Task Tracker  Database: taskmanager.Collections: projects , tasks , users 
Section 1: Basic Document Modeling & Insertion 
1.Create a database called taskmanager ,
2.Insert 3 users into a users collection. Each should have: 
name (string) 
email (string) 
role (either "admin" , "manager" , or "developer" ) 
active (boolean) 

use taskmanager
switched to db taskmanager
db.users.insertMany([
  { name: "Revi", email: "revi@example.com", role: "admin", active: true },
  { name: "Sam", email: "sam@example.com", role: "developer", active: true },
  { name: "Maddy", email: "maddy@example.com", role: "manager", active: false }
]);

Output:
{
  acknowledged: true,
  insertedIds: {
    '0': ObjectId('683062ea9bff547699291cac'),
    '1': ObjectId('683062ea9bff547699291cad'),
    '2': ObjectId('683062ea9bff547699291cae')
  }
}

3.Insert 2 projects into a projects collection: 
title , description , startDate , status (e.g. "active" , "completed" ) Embed a createdBy sub-document containing the user’s _id , name.

const Revi = db.users.findOne({ name: "Revi" });

db.projects.insertMany([
  {
    title: "Website Redesign",
    description: "Revamp the corporate website.",
    startDate: new Date("2025-05-01"),
    status: "active",
    createdBy: {
      _id: Revi._id,
      name: Revi.name
    },
    progress: 0
  },
  {
    title: "Mobile App Launch",
    description: "Launch the new mobile app.",
    startDate: new Date("2025-04-10"),
    status: "completed",
    createdBy: {
      _id: Revi._id,
      name: Revi.name
    },
    progress: 100
  }
]);

Output:
{
  acknowledged: true,
  insertedIds: {
    '0': ObjectId('683063429bff547699291caf'),
    '1': ObjectId('683063429bff547699291cb0')
  }
}

4.Insert 5 tasks into a tasks collection: 
Fields: title , assignedTo (user _id ), projectId , priority , dueDate , status. 

const project1 = db.projects.findOne({ title: "Website Redesign" });
const project2 = db.projects.findOne({ title: "Mobile App Launch" });
const sam = db.users.findOne({ name: "Sam" });

db.tasks.insertMany([
  {
    title: "Create wireframes",
    assignedTo: sam._id,
    projectId: project1._id,
    priority: "high",
    dueDate: new Date("2025-06-01"),
    status: "in-progress"
  },
  {
    title: "Develop landing page",
    assignedTo: sam._id,
    projectId: project1._id,
    priority: "medium",
    dueDate: new Date("2025-06-05"),
    status: "pending"
  },
  {
    title: "Setup CI/CD pipeline",
    assignedTo: sam._id,
    projectId: project2._id,
    priority: "high",
    dueDate: new Date("2025-05-28"),
    status: "in-progress"
  },
  {
    title: "User testing",
    assignedTo: sam._id,
    projectId: project1._id,
    priority: "low",
    dueDate: new Date("2025-06-10"),
    status: "pending"
  },
  {
    title: "Publish release notes",
    assignedTo: sam._id,
    projectId: project2._id,
    priority: "high",
    dueDate: new Date("2025-05-20"),
    status: "pending"
  }
]);

Output:
{
  acknowledged: true,
  insertedIds: {
    '0': ObjectId('683063839bff547699291cb1'),
    '1': ObjectId('683063839bff547699291cb2'),
    '2': ObjectId('683063839bff547699291cb3'),
    '3': ObjectId('683063839bff547699291cb4'),
    '4': ObjectId('683063839bff547699291cb5')
  }
}

Section 2: Filtering & Querying 
5.Find all tasks with priority "high" that are not completed

db.tasks.find({ priority: "high", status: { $ne: "completed" } });

Output:
{
  _id: ObjectId('683063839bff547699291cb1'),
  title: 'Create wireframes',
  assignedTo: ObjectId('683062ea9bff547699291cad'),
  projectId: ObjectId('683063429bff547699291caf'),
  priority: 'high',
  dueDate: 2025-06-01T00:00:00.000Z,
  status: 'in-progress'
}
{
  _id: ObjectId('683063839bff547699291cb3'),
  title: 'Setup CI/CD pipeline',
  assignedTo: ObjectId('683062ea9bff547699291cad'),
  projectId: ObjectId('683063429bff547699291cb0'),
  priority: 'high',
  dueDate: 2025-05-28T00:00:00.000Z,
  status: 'in-progress'
}
{
  _id: ObjectId('683063839bff547699291cb5'),
  title: 'Publish release notes',
  assignedTo: ObjectId('683062ea9bff547699291cad'),
  projectId: ObjectId('683063429bff547699291cb0'),
  priority: 'high',
  dueDate: 2025-05-20T00:00:00.000Z,
  status: 'pending'
}

6.Query all active users with role "developer" 

db.users.find({ role: "developer", active: true });

Output:
{
  _id: ObjectId('683062ea9bff547699291cad'),
  name: 'Sam',
  email: 'sam@example.com',
  role: 'developer',
  active: true
}

7.Find all tasks assigned to a specific user (by ObjectId )

db.tasks.find({ assignedTo: sam._id });

Output:
{
  _id: ObjectId('683063839bff547699291cb1'),
  title: 'Create wireframes',
  assignedTo: ObjectId('683062ea9bff547699291cad'),
  projectId: ObjectId('683063429bff547699291caf'),
  priority: 'high',
  dueDate: 2025-06-01T00:00:00.000Z,
  status: 'in-progress'
}
{
  _id: ObjectId('683063839bff547699291cb2'),
  title: 'Develop landing page',
  assignedTo: ObjectId('683062ea9bff547699291cad'),
  projectId: ObjectId('683063429bff547699291caf'),
  priority: 'medium',
  dueDate: 2025-06-05T00:00:00.000Z,
  status: 'pending'
}
{
  _id: ObjectId('683063839bff547699291cb3'),
  title: 'Setup CI/CD pipeline',
  assignedTo: ObjectId('683062ea9bff547699291cad'),
  projectId: ObjectId('683063429bff547699291cb0'),
  priority: 'high',
  dueDate: 2025-05-28T00:00:00.000Z,
  status: 'in-progress'
}
{
  _id: ObjectId('683063839bff547699291cb4'),
  title: 'User testing',
  assignedTo: ObjectId('683062ea9bff547699291cad'),
  projectId: ObjectId('683063429bff547699291caf'),
  priority: 'low',
  dueDate: 2025-06-10T00:00:00.000Z,
  status: 'pending'
}
{
  _id: ObjectId('683063839bff547699291cb5'),
  title: 'Publish release notes',
  assignedTo: ObjectId('683062ea9bff547699291cad'),
  projectId: ObjectId('683063429bff547699291cb0'),
  priority: 'high',
  dueDate: 2025-05-20T00:00:00.000Z,
  status: 'pending'
}
 
8.Find all projects started in the last 30 days 

const thirtyDaysAgo = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000);
db.projects.find({ startDate: { $gte: thirtyDaysAgo } });

Output:
{
  _id: ObjectId('683063429bff547699291caf'),
  title: 'Website Redesign',
  description: 'Revamp the corporate website.',
  startDate: 2025-05-01T00:00:00.000Z,
  status: 'active',
  createdBy: {
    _id: ObjectId('683062ea9bff547699291cac'),
    name: 'Revi'
  },
  progress: 0
}

Section 3: Update Operations 
9.  Change the status of one task to "completed" 

db.tasks.updateOne(
  { title: "Create wireframes" },
  { $set: { status: "completed" } }
);

Output:
{
  acknowledged: true,
  insertedId: null,
  matchedCount: 1,
  modifiedCount: 1,
  upsertedCount: 0
}

10.Add a new role field called "teamLead" to one of the users

db.users.updateOne({ name: "Maddy" }, { $set: { role: "teamLead" } });

Output:
{
  acknowledged: true,
  insertedId: null,
  matchedCount: 1,
  modifiedCount: 1,
  upsertedCount: 0
}

11.  Add a new tag array to a task: ["urgent", "frontend"] 

db.users.updateOne({ name: "Maddy" }, { $set: { role: "teamLead" } });

Output:
{
  acknowledged: true,
  insertedId: null,
  matchedCount: 1,
  modifiedCount: 1,
  upsertedCount: 0
}

Section 4: Array and Subdocument Operations 
12.  Add a new tag "UI" to the task’s tags array using $addToSet 

db.tasks.updateOne(
  { title: "Develop landing page" },
  { $addToSet: { tags: "UI" } }
);

Output:
{
  acknowledged: true,
  insertedId: null,
  matchedCount: 1,
  modifiedCount: 1,
  upsertedCount: 0
}

13.Remove "frontend" from a task’s tag list 

db.tasks.updateOne(
  { title: "Develop landing page" },
  { $pull: { tags: "frontend" } }
);
Output:
{
  acknowledged: true,
  insertedId: null,
  matchedCount: 1,
  modifiedCount: 1,
  upsertedCount: 0
}


14.Use $inc to increment a project ’s progress field by 10

db.projects.updateOne(
  { title: "Website Redesign" },
  { $inc: { progress: 10 } }
);

Output:
{
  acknowledged: true,
  insertedId: null,
  matchedCount: 1,
  modifiedCount: 1,
  upsertedCount: 0
}

Section 5: Aggregation & Lookup 
15.Use $lookup to join tasks with users and show task title + assignee name 

db.tasks.aggregate([
  {
    $lookup: {
      from: "users",
      localField: "assignedTo",
      foreignField: "_id",
      as: "assignee"
    }
  },
  { $unwind: "$assignee" },
  {
    $project: {
      _id: 0,
      title: 1,
      assigneeName: "$assignee.name"
    }
  }
]);

Output:
{
  title: 'Create wireframes',
  assigneeName: 'Sam'
}
{
  title: 'Develop landing page',
  assigneeName: 'Sam'
}
{
  title: 'Setup CI/CD pipeline',
  assigneeName: 'Sam'
}
{
  title: 'User testing',
  assigneeName: 'Sam'
}
{
  title: 'Publish release notes',
  assigneeName: 'Sam'
}

16.Use $lookup to join tasks with projects , and filter tasks where project status = active

db.tasks.aggregate([
  {
    $lookup: {
      from: "projects",
      localField: "projectId",
      foreignField: "_id",
      as: "project"
    }
  },
  { $unwind: "$project" },
  { $match: { "project.status": "active" } },
  {
    $project: {
      _id: 0,
      title: 1,
      projectTitle: "$project.title"
    }
  }
]);

Output:
{
  title: 'Create wireframes',
  projectTitle: 'Website Redesign'
}
{
  title: 'Develop landing page',
  projectTitle: 'Website Redesign'
}
{
  title: 'User testing',
  projectTitle: 'Website Redesign'
}

17.Use $group to get count of tasks per status 

db.tasks.aggregate([
  {
    $group: {
      _id: "$status",
      count: { $sum: 1 }
    }
  }
]);

Output:
{
  _id: 'completed',
  count: 1
}
{
  _id: 'pending',
  count: 3
}
{
  _id: 'in-progress',
  count: 1
}
18.Use $match , $sort , and $limit to get top 3 soonest due tasks

db.tasks.aggregate([
  { $match: { dueDate: { $exists: true } } },
  { $sort: { dueDate: 1 } },
  { $limit: 3 },
  {
    $project: {
      _id: 0,
      title: 1,
      dueDate: 1
    }
  }
]);

Output:
{
  title: 'Publish release notes',
  dueDate: 2025-05-20T00:00:00.000Z
}
{
  title: 'Setup CI/CD pipeline',
  dueDate: 2025-05-28T00:00:00.000Z
}
{
  title: 'Create wireframes',
  dueDate: 2025-06-01T00:00:00.000Z
}




 






