
class Coche:
    def __init__(self,marca , modelo):
        self.marca = marca
        self.modelo = modelo


    def promocionar(self, year):
        
        print(f"Soy el auto de marca {self.marca} y modelo {self.modelo} del anio {year}")


coche1 = Coche("Nissan","Santa Fe")

print(coche1.marca)

print(coche1.modelo)

print(coche1.year)

coche1.promocionar(2005)

