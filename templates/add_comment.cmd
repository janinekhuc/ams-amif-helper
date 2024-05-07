for %%f in (*.sql) do (
    echo "-- Databricks notebook source" | type "%%f" > temp & move /Y temp "%%f"
)