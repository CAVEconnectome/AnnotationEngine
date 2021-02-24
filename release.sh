#!/bin/bash
bumpversion $1
git push && git push --tags
TAG=$(git describe --tags)
