import random
target = random.randint(1, 100)
guess = None

while guess != target:
    guess = int(input("Guess a number between 1 and 100: "))
    if guess < target:
        print("Too low")
    elif guess > target:
        print("Too high")
    else:
        print("Correct!")
