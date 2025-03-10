import string
import concurrent.futures
from collections import defaultdict, Counter
import requests
from matplotlib import pyplot as plt

# Функція для видалення пунктуації та приведення до нижнього регістру
def clean_text(text):
    text = text.lower()
    return text.translate(str.maketrans("", "", string.punctuation))

# Функція маппінгу
def map_function(word):
    return word, 1

# Shuffle phase
def shuffle_function(mapped_values):
    shuffled = defaultdict(list)
    for key, value in mapped_values:
        shuffled[key].append(value)
    return shuffled.items()

# Reduce phase
def reduce_function(key_values):
    key, values = key_values
    return key, sum(values)

# MapReduce процес
def map_reduce(url):
    response = requests.get(url)
    
    if response.status_code == 200:
        text = response.text

        # Чистимо текст
        text = clean_text(text)
        words = text.split()

        # Мапінг
        with concurrent.futures.ThreadPoolExecutor() as executor:
            mapped_values = list(executor.map(map_function, words))

        # Shuffle
        shuffled_values = shuffle_function(mapped_values)

        # Редукція
        with concurrent.futures.ThreadPoolExecutor() as executor:
            reduced_values = list(executor.map(reduce_function, shuffled_values))

        return dict(reduced_values)
    else:
        print(f"Error fetching data: {response.status_code}")
        return None

# Візуалізація
def visualize_top_words(word_counts, top_n=10):
    top_words = Counter(word_counts).most_common(top_n)
    words, frequencies = zip(*top_words)

    plt.figure(figsize=(12, 7))
    plt.barh(words, frequencies, color='skyblue')
    plt.xlabel('Frequency')
    plt.ylabel('Words')
    plt.title(f'Top {top_n} Most Frequent Words')
    plt.gca().invert_yaxis() 
    plt.show()

# Основний блок
if __name__ == '__main__':
    url = "https://gutenberg.net.au/ebooks01/0100021.txt"
    word_frequencies = map_reduce(url)

    if word_frequencies:
        print("Top 10 words with their frequencies:")
        for word, freq in Counter(word_frequencies).most_common(10):
            print(f"{word}: {freq}")

        visualize_top_words(word_frequencies)
