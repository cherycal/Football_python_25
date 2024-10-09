__author__ = 'chance'
import operator

class Item:
	def __init__(self, name, price, discount):
		self.name = name
		self.price = price
		self.discount = discount

	def __repr__(self):
		return '{' + self.name + ', ' + str(self.price) + ', ' + str(self.discount) + '}'


# create a list of objects
sample_list = [Item("Doritos", 3, 10),
        Item("Crisps & Chips", 5, 3),
        Item("Cheetos", 4.48, 5),
        Item("Cheese Balls", 6.58, 8),
        Item("Pringles", 1.68, 2)]


def getKey(obj):
	return obj.name


def main():
	# printing the list before sorting
	print("Before sorting:")
	print(sample_list)

	# method 1
	sample_list.sort(key=getKey)
	print("Sort by Name")
	print(sample_list)

	# method 2
	sample_list.sort(key=lambda x: x.price)
	print("Sort by price")
	print(sample_list)

	# method 3
	sample_list.sort(key=operator.attrgetter('discount'))
	print("Sort by discount")
	print(sample_list)


if __name__ == "__main__":
	main()
