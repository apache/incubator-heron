# -*- coding: utf-8 -*-

import re

_REGEX = re.compile('^(?P<major>(?:0|[1-9][0-9]*))'
                    '\.(?P<minor>(?:0|[1-9][0-9]*))'
                    '\.(?P<patch>(?:0|[1-9][0-9]*))'
                    '(\-(?P<prerelease>[0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*))?'
                    '(\+(?P<build>[0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*))?$')

_LAST_NUMBER = re.compile(r'(?:[^\d]*(\d+)[^\d]*)+')

if not hasattr(__builtins__, 'cmp'):
    cmp = lambda a, b: (a > b) - (a < b)


def parse(version):
    """
    Parse version to major, minor, patch, pre-release, build parts.
    """
    match = _REGEX.match(version)
    if match is None:
        raise ValueError('%s is not valid SemVer string' % version)

    verinfo = match.groupdict()

    verinfo['major'] = int(verinfo['major'])
    verinfo['minor'] = int(verinfo['minor'])
    verinfo['patch'] = int(verinfo['patch'])

    return verinfo


def compare(ver1, ver2):
    def nat_cmp(a, b):
        a, b = a or '', b or ''
        convert = lambda text: (2, int(text)) if re.match('[0-9]+', text) else (1, text)
        split_key = lambda key: [convert(c) for c in key.split('.')]
        return cmp(split_key(a), split_key(b))

    def compare_by_keys(d1, d2):
        for key in ['major', 'minor', 'patch']:
            v = cmp(d1.get(key), d2.get(key))
            if v:
                return v

        rc1, rc2 = d1.get('prerelease'), d2.get('prerelease')
        rccmp = nat_cmp(rc1, rc2)

        build_1, build_2 = d1.get('build'), d2.get('build')
        build_cmp = nat_cmp(build_1, build_2)

        if not rccmp and not build_cmp:
            return 0
        if not rc1 and not build_1:
            return 1
        elif not rc2 and not build_2:
            return -1

        return rccmp or build_cmp

    v1, v2 = parse(ver1), parse(ver2)

    return compare_by_keys(v1, v2)


def match(version, match_expr):
    prefix = match_expr[:2]
    if prefix in ('>=', '<=', '=='):
        match_version = match_expr[2:]
    elif prefix and prefix[0] in ('>', '<', '='):
        prefix = prefix[0]
        match_version = match_expr[1:]
    else:
        raise ValueError("match_expr parameter should be in format <op><ver>, "
                         "where <op> is one of ['<', '>', '==', '<=', '>=']. "
                         f"You provided: {match_expr!r}")

    possibilities_dict = {
        '>': (1,),
        '<': (-1,),
        '==': (0,),
        '>=': (0, 1),
        '<=': (-1, 0)
    }

    possibilities = possibilities_dict[prefix]
    cmp_res = compare(version, match_version)

    return cmp_res in possibilities


def max_ver(ver1, ver2):
    cmp_res = compare(ver1, ver2)
    if cmp_res == 0 or cmp_res == 1:
        return ver1
    else:
        return ver2


def min_ver(ver1, ver2):
    cmp_res = compare(ver1, ver2)
    if cmp_res == 0 or cmp_res == -1:
        return ver1
    else:
        return ver2


def format_version(major, minor, patch, prerelease=None, build=None):
    version = f"{int(major)}.{int(minor)}.{int(patch)}"
    if prerelease is not None:
        version = version + f"-{prerelease}"

    if build is not None:
        version = version + f"+{build}"

    return version


def _increment_string(string):
    # look for the last sequence of number(s) in a string and increment, from:
    # http://code.activestate.com/recipes/442460-increment-numbers-in-a-string/#c1
    match = _LAST_NUMBER.search(string)
    if match:
        next_ = str(int(match.group(1))+1)
        start, end = match.span(1)
        string = string[:max(end - len(next_), start)] + next_ + string[end:]
    return string


def bump_major(version):
    verinfo = parse(version)
    return format_version(verinfo['major'] + 1, 0, 0)

def bump_minor(version):
    verinfo = parse(version)
    return format_version(verinfo['major'], verinfo['minor'] + 1, 0)

def bump_patch(version):
    verinfo = parse(version)
    return format_version(verinfo['major'], verinfo['minor'], verinfo['patch'] + 1)

def bump_prerelease(version):
    verinfo = parse(version)
    verinfo['prerelease'] = _increment_string(verinfo['prerelease'] or 'rc.0')
    return format_version(verinfo['major'], verinfo['minor'], verinfo['patch'],
                          verinfo['prerelease'])

def bump_build(version):
    verinfo = parse(version)
    verinfo['build'] = _increment_string(verinfo['build'] or 'build.0')
    return format_version(verinfo['major'], verinfo['minor'], verinfo['patch'],
                          verinfo['prerelease'], verinfo['build'])
