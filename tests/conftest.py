def pytest_report_teststatus(report):
    if report.when == 'call':
        if report.passed:
            letter = "."
        elif report.skipped:
            letter = "s"
        elif report.failed:
            letter = "x"
        return (report.outcome, letter,
                '{} ({:.2f}s)'.format(report.outcome.upper(), report.duration))
