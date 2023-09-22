

if __name__ == "__main__":
    stocks_file_path = "data/stocks"

    for i in range(10):
        file_name = f"{stocks_file_path}/{id}.csv"

        with open(file_name, "w") as file:
            data = "AAPL,2023.3.20,157.8200073\nAAPL,2023.3.21,159.3999939\nAAPL,2023.3.22,162.1399994\nAAPL,2023.3.23,161.5500031\nAAPL,2023.3.24,160.3399963\nAAPL,2023.3.27,160.7700043"
            file.write(data)

            print(f"{file_name} is written")