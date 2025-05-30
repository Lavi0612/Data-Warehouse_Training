# q5_salary_analysis.py
salaries = list(map(int, input("Enter salaries separated by commas: ").split(',')))
print("Maximum:", max(salaries))
average = sum(salaries) / len(salaries)
print("Above Average:", [s for s in salaries if s > average])
print("Sorted Desc:", sorted(salaries, reverse=True))
