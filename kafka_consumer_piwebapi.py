"""
Main entry point. Just calls the consume function in the consumer_piwebapi module.

TODO: Make this run as a Windows Service
https://www.thepythoncorner.com/2018/08/how-to-create-a-windows-service-in-python/
"""

from src.consumer_piwebapi import consume

if __name__ == '__main__':
    consume()