# Contributing to statsdcat

Thank you for contributing to `statsdcat`!

## Guidelines

1. **Standalone Crate**: `statsdcat` is an independent crate with no workspace dependencies.
2. **Quality & Tests**: Ensure all code compiles cleanly without warnings and that existing and new tests pass:
   ```bash
   cargo test
   ```
3. **No External Daemons**: `statsdcat` runs entirely in userspace with zero external runtime requirements.
4. **License & Attribution**: Contributions are licensed under the MIT License. Please do not add automated or AI attribution in code comments or commit logs.
