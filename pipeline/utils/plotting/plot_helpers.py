


def hex_to_rgb(hex_color):
    hex_color = hex_color.lstrip("#")
    return tuple(int(hex_color[i:i+2], 16) for i in (0, 2, 4))


def rgb_to_hex(rgb):
    return "#{:02X}{:02X}{:02X}".format(*rgb)


def interpolate(c1, c2, t):
    return tuple(
        int(c1[i] + (c2[i] - c1[i]) * t)
        for i in range(3)
    )


ANCHORS = [
    "#4C6EF5",  # install
    "#38D9A9",  # engagement
    "#FCC419",  # intent
    "#2F9E44",  # conversion
]


def funnel_gradient(n_steps):
    if n_steps <= 1:
        return [ANCHORS[-1]]

    anchors_rgb = [hex_to_rgb(c) for c in ANCHORS]
    segments = len(anchors_rgb) - 1
    colors = []

    for i in range(n_steps):
        t = i / (n_steps - 1)
        seg = min(int(t * segments), segments - 1)
        local_t = (t - seg / segments) * segments

        c = interpolate(
            anchors_rgb[seg],
            anchors_rgb[seg + 1],
            local_t
        )
        colors.append(rgb_to_hex(c))

    return colors
