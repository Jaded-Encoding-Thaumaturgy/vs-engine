# vs-jet-engine

[![Lint](https://github.com/Jaded-Encoding-Thaumaturgy/vs-engine/actions/workflows/lint.yml/badge.svg)](https://github.com/Jaded-Encoding-Thaumaturgy/vs-engine/actions/workflows/lint.yml)
[![Tests](https://github.com/Jaded-Encoding-Thaumaturgy/vs-engine/actions/workflows/test.yml/badge.svg)](https://github.com/Jaded-Encoding-Thaumaturgy/vs-engine/actions/workflows/test.yml)
[![Coverage Status](https://coveralls.io/repos/github/Jaded-Encoding-Thaumaturgy/vs-engine/badge.svg?branch=main)](https://coveralls.io/github/Jaded-Encoding-Thaumaturgy/vs-engine?branch=main)

An engine for vapoursynth previewers, renderers and script analyis tools.

## Installation

```
pip install vsjetengine
```

## Using vsengine

Look at this example:

```py
import vapoursynth as vs
from vsengine.vpy import script

script("/script/to/my.vpy").result()
vs.get_output(0).output()
```

## Contributing

This project is licensed under the EUPL-1.2.
When contributing to this project you accept that your code will be using this license.
By contributing you also accept any relicencing to newer versions of the EUPL at a later point in time.
