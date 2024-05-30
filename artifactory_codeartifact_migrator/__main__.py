#!/usr/bin/env python3
# Copyright 2022 Shawn Qureshi and individual contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import sys

from typing import Any

from . import cli


def main() -> Any:
    try:
        error = cli.dispatch(sys.argv[1:])
    except Exception as exc:
        error = True
        print(f"{exc}", file=sys.stderr)

    return error


if __name__ == "__main__":
    sys.exit(main())
