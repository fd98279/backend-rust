# imports
import ast
import glob
import os  # for environment variables
from itertools import chain
from pathlib import Path

import pandas as pd  # for DataFrames to store article sections and embeddings
import tiktoken  # for counting tokens
from openai import OpenAI  # for generating embeddings
from scipy import spatial

# Specify the directory path
current_script_directory = os.path.dirname(os.path.realpath(__file__))
data_directory = os.path.join(current_script_directory, '../../data/mutual_funds/')
embeddings_output_file = os.path.join(current_script_directory, '../../data/mutual_funds/embeddings.csv')
client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY", os.environ.get("OPENAI_API_KEY")))
GPT_MODEL = "gpt-3.5-turbo"  # only matters insofar as it selects which tokenizer to use
# split sections into chunks
MAX_TOKENS = 4096 # gpt-3.5-turbo max size
EMBEDDING_MODEL = "text-embedding-3-small"
BATCH_SIZE = 1000  # you can submit up to 2048 embedding inputs per request


# Use glob to find all JSON files in the directory
json_files = glob.glob(f'{data_directory}/*.json')
token_strings = []

def num_tokens(text: str, model: str = GPT_MODEL) -> int:
    """Return the number of tokens in a string."""
    encoding = tiktoken.encoding_for_model(model)
    return len(encoding.encode(text))

def chunk_string(input_string, chunk_size):
    """
    Split a string into chunks of a given size.

    Parameters:
    - input_string: The input string to be split.
    - chunk_size: The size of each chunk.

    Returns:
    - A list containing the chunks.
    """
    return [input_string[i:i + chunk_size] for i in range(0, len(input_string), chunk_size)]

def get_tokens_for_chunks(file_path, chunk_identifier, chunks):
    for index, chunk in enumerate(chunks):
        chunk = f"{chunk_identifier}{chunk}"
        token_count = num_tokens(chunk, GPT_MODEL)
        print(f"Filename: {file_path} - Chunk: {index} of {len(chunks)}: Num tokens: {token_count}")
        if token_count > MAX_TOKENS:
            raise ValueError(f"Token count exeeds MAX_TOKEN: {token_count} - {MAX_TOKENS}")


def create_embeddings():
    all_file_chunks = []
    for file_path in json_files:
        with open(file_path, 'r') as file:
            json_string = file.read()
            path_object = Path(file_path)
            filename_without_extension = path_object.stem
            chunk_identifier = f"Mutual fund name: {filename_without_extension}: Details: "
            chunk_identifier_tokens = num_tokens(chunk_identifier, GPT_MODEL)
            print(f"Chunk identifier token count: {chunk_identifier_tokens}")
            total_tokens = num_tokens(json_string, GPT_MODEL)

            total_chunks = math.ceil(total_tokens/MAX_TOKENS)
            print(f"Total tokens: {total_tokens} - Total Chunks: {total_chunks}")
            chunks = chunk_string(json_string, int(len(json_string)/total_chunks))

            try:
                _ = get_tokens_for_chunks(file_path, chunk_identifier, chunks)
            except ValueError:
                total_chunks = total_chunks + 1
                print(f"Total tokens: {total_tokens} - Total Chunks: {total_chunks}")
                chunks = chunk_string(json_string, int(len(json_string)/total_chunks))    
                _ = get_tokens_for_chunks(file_path, chunk_identifier, chunks)
        
            all_file_chunks.append(chunks)

    all_file_chunks = list(chain.from_iterable(all_file_chunks))

    embeddings = []
    for batch_start in range(0, len(all_file_chunks), BATCH_SIZE):
        batch_end = batch_start + BATCH_SIZE
        batch = all_file_chunks[batch_start:batch_end]
        print(f"Batch {batch_start} to {batch_end-1}")
        response = client.embeddings.create(model=EMBEDDING_MODEL, input=batch)
        for i, be in enumerate(response.data):
            assert i == be.index  # double check embeddings are in same order as input
        batch_embeddings = [e.embedding for e in response.data]
        embeddings.extend(batch_embeddings)

    df = pd.DataFrame({"text": all_file_chunks, "embedding": embeddings})
    df.to_csv(embeddings_output_file, index=False)
    
# search function
def strings_ranked_by_relatedness(
    query: str,
    df: pd.DataFrame,
    relatedness_fn=lambda x, y: 1 - spatial.distance.cosine(x, y),
    top_n: int = 100):
    """Returns a list of strings and relatednesses, sorted from most related to least."""
    query_embedding_response = client.embeddings.create(
        model=EMBEDDING_MODEL,
        input=query,
    )
    query_embedding = query_embedding_response.data[0].embedding
    strings_and_relatednesses = [
        (row["text"], relatedness_fn(query_embedding, row["embedding"]))
        for i, row in df.iterrows()
    ]
    strings_and_relatednesses.sort(key=lambda x: x[1], reverse=True)
    strings, relatednesses = zip(*strings_and_relatednesses)
    return strings[:top_n], relatednesses[:top_n]

def num_tokens(text: str, model: str = GPT_MODEL) -> int:
    """Return the number of tokens in a string."""
    encoding = tiktoken.encoding_for_model(model)
    return len(encoding.encode(text))


def query_message(
    query: str,
    df: pd.DataFrame,
    model: str,
    token_budget: int
) -> str:
    """Return a message for GPT, with relevant source texts pulled from a dataframe."""
    strings, relatednesses = strings_ranked_by_relatedness(query, df)
    introduction = '''Use the below Mutual funds information
    to answer the subsequent question. If the answer cannot be found in the information, 
    write "I could not find an answer."'''
    question = f"\n\nQuestion: {query}"
    message = introduction
    for string in strings:
        next_article = f'\n\nMutual Funds Information:\n"""\n{string}\n"""'
        if (
            num_tokens(message + next_article + question, model=model)
            > token_budget
        ):
            break
        else:
            message += next_article
    return message + question


def ask(
    query: str,
    df: pd.DataFrame,
    model: str = GPT_MODEL,
    token_budget: int = 4096 - 500,
    print_message: bool = False,
) -> str:
    """Answers a query using GPT and a dataframe of relevant texts and embeddings."""
    message = query_message(query, df, model=model, token_budget=token_budget)
    if print_message:
        print(message)
    messages = [
        {"role": "system", "content": "You answer questions about mutual funds."},
        {"role": "user", "content": message},
    ]
    response = client.chat.completions.create(
        model=model,
        messages=messages,
        temperature=0
    )
    response_message = response.choices[0].message.content
    return response_message

if __name__ == '__main__':
    # create_embeddings()
    df = pd.read_csv(embeddings_output_file)
    # convert embeddings from CSV str type back to list type
    df['embedding'] = df['embedding'].apply(ast.literal_eval)
    # the dataframe has two columns: "text" and "embedding"
    # print(df)
        
    # examples
    #strings, relatednesses = strings_ranked_by_relatedness("AAAAX", df, top_n=5)
    #for string, relatedness in zip(strings, relatednesses):
    #    print(f"{relatedness=:.3f}")
        # print(string)

    # print(ask('What is OpenFigi of Alpha Alternative Assets Fund', df=df, print_message=True)) #N
    # print(ask("For BBG000RSJVZ1 what is the Fund Summary", df=df, print_message=True)) #Y
    # print(ask("For AAACX what are top 10 holdings", df=df, print_message=True)) #Y
    # print(ask("For AAACX what are bottom 10 holdings", df=df, print_message=True)) #Y
    # print(ask("For AAACX what are bottom 10 holdings", df=df, print_message=True)) #Y
    # print(ask("For AAAAX what are the Asset Allocation", df=df, print_message=True)) #N
    # print(ask("For AAAAX DWS RREEF Real Assets Fund - Class A what are the Top_Holdings", df=df, print_message=True)) #N
    # print(ask("Which fund has Portfolio_Net_Assets=5165680000", df=df, print_message=True)) #Y
    print(ask("For mutual funds with code AAAAX.US and AAACX.US which has the highest AverageMarketCap", df=df, print_message=True))
    
    
