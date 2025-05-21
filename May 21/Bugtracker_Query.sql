Bug Tracker System

Database: bugtracker Collection: bugs

1. Create a new database called bugtracker

use bugtracker

Output:

switched to db bugtracker

2. Insert 3 bug reports into a bugs collection. Each document should include:
title (string)
reportedBy (string)
status (either "open", "closed", "in progress")
priority (string "low", "medium", "high")
createdAt (date)

db.bugs.insertMany([
  {
    title: "Login fails with 500 error",
    reportedBy: "Ali",
    status: "open",
    priority: "high",
    createdAt: new Date("2025-05-20")
  },
  {
    title: "UI not responsive on mobile",
    reportedBy: "Bob",
    status: "in progress",
    priority: "medium",
    createdAt: new Date("2025-05-18")
  },
  {
    title: "Typo on homepage",
    reportedBy: "Test User",
    status: "open",
    priority: "low",
    createdAt: new Date("2025-05-21")
  }
])

Output:

{
  acknowledged: true,
  insertedIds: {
    '0': ObjectId('682db7ee0705b345ea2f2c91'),
    '1': ObjectId('682db7ee0705b345ea2f2c92'),
    '2': ObjectId('682db7ee0705b345ea2f2c93')
  }
}

3. Query all bugs with status: "open" and priority: "high"

db.bugs.find({
  status: "open",
  priority: "high"
})
Output:

{
  _id: ObjectId('682db7ee0705b345ea2f2c91'),
  title: 'Login fails with 500 error',
  reportedBy: 'Ali',
  status: 'open',
  priority: 'high',
  createdAt: 2025-05-20T00:00:00.000Z
}

4. Update the status of a specific bug to "closed"

db.bugs.updateOne(
  { title: "Login fails with 500 error" },
  { $set: { status: "closed" } }
)

Output:

{
  acknowledged: true,
  insertedId: null,
  matchedCount: 1,
  modifiedCount: 1,
  upsertedCount: 0
}

5. Delete the bug that was reported by "Test User"

db.bugs.deleteOne({ reportedBy: "Test User" })

Output:

{
  acknowledged: true,
  deletedCount: 1
}


