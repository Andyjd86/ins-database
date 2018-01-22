# -*- coding: utf-8 -*-
"""
/***************************************************************************
 RouteBuilder
                                 A QGIS plugin
 Tool to build routes
                             -------------------
        begin                : 2018-01-22
        copyright            : (C) 2018 by Andy Dixon
        email                : adixon@trl.co.uk
        git sha              : $Format:%H$
 ***************************************************************************/

/***************************************************************************
 *                                                                         *
 *   This program is free software; you can redistribute it and/or modify  *
 *   it under the terms of the GNU General Public License as published by  *
 *   the Free Software Foundation; either version 2 of the License, or     *
 *   (at your option) any later version.                                   *
 *                                                                         *
 ***************************************************************************/
 This script initializes the plugin, making it known to QGIS.
"""


# noinspection PyPep8Naming
def classFactory(iface):  # pylint: disable=invalid-name
    """Load RouteBuilder class from file RouteBuilder.

    :param iface: A QGIS interface instance.
    :type iface: QgsInterface
    """
    #
    from .route_builder import RouteBuilder
    return RouteBuilder(iface)
