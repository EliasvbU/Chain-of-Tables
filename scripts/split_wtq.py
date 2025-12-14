import random

def split_dataset(data, train_ratio=0.8, val_ratio=0.1, seed=42):
    random.seed(seed)
    random.shuffle(data)

    n = len(data)
    train_end = int(n * train_ratio)
    val_end = int(n * (train_ratio + val_ratio))

    return {
        "train": data[:train_end],
        "val": data[train_end:val_end],
        "test": data[val_end:]
    }
