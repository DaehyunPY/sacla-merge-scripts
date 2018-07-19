from .daemon import run

try:
    run()
except KeyboardInterrupt:
    print('Bye!')
