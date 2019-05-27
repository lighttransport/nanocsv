#!/usr/bin/env python

import numpy as np

r = np.random.rand(4096, 4096).astype('float32')
np.savetxt('data.txt', r)
