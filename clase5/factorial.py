# 5! = 120
# f(x) = x*f(x-1) = x*(x-1)*f(x-2)*...

def calcular():
    return 0

class Factorial:

    def __init__(self , n):
        self.n = n

    def calcular(self):
        
        m = "buena"
        print(m)
        
        temp = self.n

        if (self.n ==1 ):
            return 1

        self.n = self.n-1

        return  temp * self.calcular()

print(Factorial(9).calcular())

