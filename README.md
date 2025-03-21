# kvstore

**kvstore** is a simple, efficient, and lightweight key-value store written in Rust. It provides a straightforward API for storing, retrieving, and managing key-value pairs, making it suitable for various applications requiring quick data storage and retrieval.

## Features

- **In-Memory Storage**: Fast operations with in-memory data handling.
- **Persistent Storage**: Optional disk-based storage for data persistence across restarts.
- **Concurrency**: Thread-safe operations to handle multiple simultaneous requests.
- **Lightweight**: Minimal dependencies ensure a small footprint and quick startup times.

## Installation

To use `kvstore`, ensure you have [Rust](https://www.rust-lang.org/) installed. Then, add `kvstore` to your `Cargo.toml`:

```toml
[dependencies]
kvstore = "0.1.0"
```

Replace `"0.1.0"` with the latest version if different.

## Usage

Here's a basic example of how to use `kvstore`:

```rust
use kvstore::KvStoreBuilder;

fn main() {
  // Create a new key-value store
  let store = KVStoreBuilder::new().build("s3://kvstore").await?;

  // Set a key-value pair
  store.set("key1", "value1");

  // Get the value associated with a key
  if let Some(value) = store.get("key1") {
    println!("value={}", value);
  } else {
    println!("no value");
  }

  // Remove a key-value pair
  store.remove("key1");
}
```

For more detailed examples, refer to the [`examples`](https://github.com/haruband/kvstore/tree/main/examples) directory.

## Contributing

Contributions are welcome! Please follow these steps:

1. Fork the repository.
2. Create a new branch: `git checkout -b feature-name`.
3. Commit your changes: `git commit -m 'Add feature'`.
4. Push to the branch: `git push origin feature-name`.
5. Open a pull request detailing your changes.

Please ensure all tests pass and adhere to the existing code style.

## License

This project is licensed under the Apache-2.0 License. See the [`LICENSE`](https://github.com/haruband/kvstore/blob/main/LICENSE) file for details.

## Acknowledgements

Special thanks to the Rust community and contributors to open-source projects that inspired and assisted in the development of `kvstore`.
