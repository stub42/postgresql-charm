import subprocess

from testtools.content import text_content


__all__ = ['run']


def run(detail_collector, cmd, input=''):
    try:
        proc = subprocess.Popen(
            cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE)
    except subprocess.CalledProcessError, x:
        raise

    (out, err) = proc.communicate(input)
    if out:
        detail_collector.addDetail('stdout', text_content(out))
    if err:
        detail_collector.addDetail('stderr', text_content(err))
    if proc.returncode != 0:
        raise subprocess.CalledProcessError(
            proc.returncode, cmd, err)
    return out

