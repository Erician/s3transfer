#encoding=utf-8

class TaskIDIsNotTheSame(Exception):
    pass

class GenerateTaskFailed(Exception):
    pass


if __name__ == '__main__':
    try:
        raise Exception('err')
    except (TaskIDIsNotTheSame, Exception), e:
        print e