# hartree_partners_exercise
Repo to showcase my skills in solving the exercise provided.

# Data Processing with Pandas and Apache Beam

This repository contains code for joining two datasets and calculating various statistics using two different Python frameworks: Pandas and Apache Beam.

## Description

The project includes two scripts: one utilizing Pandas, a powerful data manipulation library, and another using Apache Beam for distributed data processing. Both scripts perform a join on two datasets and calculate metrics such as the maximum rating by counterparty and sum values based on status conditions.

## Getting Started

### Dependencies

- Python 3.8 or higher
- Pandas library
- Apache Beam library

### Installing

1. Clone the repository to your local machine:

```
git clone https://github.com/your-username/hartree_partners_exercise.git
```

2. Navigate to the cloned directory:

```
cd hartree_partners_exercise
```

3. Install the required dependencies:

```
pip3 install -r requirements.txt
```

### Executing the scripts

1. To run the Pandas script:

```
python3 src/pandas_script.py --dataset1 data/dataset1.csv --dataset2 data/dataset2.csv --output output/output.csv
```

2. To run the Apache Beam script:

```
python3 src/beam_script.py --dataset1 data/dataset1.csv --dataset2 data/dataset2.csv --output output/output.csv
```

## Authors

Contributors names and contact info

- Ralph Legge

## Version History

* 0.1
    * Initial Release

## License

This project is licensed under the GNU GENERAL PUBLIC LICENSE Version 3, 29 June 2007 - see the LICENSE.md file for details

## Acknowledgments

Inspiration, code snippets, etc.
* [pandas documentation](https://pandas.pydata.org/pandas-docs/stable/index.html)
* [Apache Beam Python SDK](https://beam.apache.org/documentation/sdks/python/)
