[![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/coiled/dask-tutorial/HEAD) 
## Dask Tutorial by Coiled 

Hands-on 1 hour sessions to learn how to scale workflows using Dask and Coiled. Each session is independent of the others, so you can choose the ones you want or come to all of them. 

#### Sign up for the next live session https://www.coiled.io/tutorials
--------------------------------------------------------------------------------------------------------------------------------------------------------

### Parallelize your Python Code: Futures API

In this lesson, we’ll parallelize a custom Python workflow that scrapes, parses, and cleans data from Stack Overflow. We’ll get to:
‍
- Learn how to do arbitrary task scheduling using the Dask Futures API
- Utilize blocking and non-blocking distributed calculations

By the end, we’ll see how much faster this workflow is using Dask and how the Dask Futures API is particularly well-suited for this type of fine-grained execution.

--------------------------------------------------------------------------------------------------------------------------------------------------------

### Get Better at Dask Dataframes

In this lesson, we’ll learn some best practices around working with larger-than-memory datasets. We’ll use the Uber/Lyft dataset to:
‍
- Manipulate Parquet files and optimize queries
- Navigate inconvenient file sizes and data types
- Extract useful features with Dask Dataframe

By the end, we’ll learn the advantages of working with the Parquet file format and how to efficiently perform an exploratory analysis with Dask.

--------------------------------------------------------------------------------------------------------------------------------------------------------

### Setup 

We have two options for you to follow this tutorial:

### Run locally

If you are joining for a live session please make sure you do the setup in advance, and be ready to go once the session starts.

1. **Clone this repository**
    In your terminal:

    ```
    git clone https://github.com/coiled/dask-tutorial.git
    cd dask-tutorial
    ```
    Alternatively, you can download the zip file of the repository at the top of the main page of the repository. This is a good option if you don't have experience with git.

2. **Create Conda Environment**

    In your terminal navigate to the directory where you have cloned/downloaded the `dask-tutorial` repository and install the required packages:

    ```
    conda env create -f binder/environment.yml
    ```

    This will create a new environment called `dask-tutorial`. To activate the environment do:

    ```
    conda activate dask-tutorial
    ```

4. **Open Jupyter Lab**

    Once your environment has been activated and you are in the `dask-tutorial` repository, in your terminal do:

    ```
    jupyter lab
    ```

    You will see a notebooks directory, click on there and you will be ready to go.

### Use Coiled notebooks

```
pip install coiled jupyter
coiled login --token ### --account dask-tutorials
coiled notebook up --software dask-tutorials
```

### Run on binder

Click on this button [![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/coiled/dask-tutorial/HEAD) to run in a pre-configured cloud environment.

If you are joining the live session, please click on the button few minutes before we start so we are ready to go.

