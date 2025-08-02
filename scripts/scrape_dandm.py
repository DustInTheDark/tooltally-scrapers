from tooltally.spiders import dandm_spider

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

if __name__ == "__main__":
    dandm_spider.main()