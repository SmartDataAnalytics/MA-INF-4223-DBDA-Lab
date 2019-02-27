/*
 *    This program is free software; you can redistribute it and/or modify
 *    it under the terms of the GNU General Public License as published by
 *    the Free Software Foundation; either version 2 of the License, or
 *    (at your option) any later version.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU General Public License for more details.
 *
 *    Copyright (C) 2015 George Antony Papadakis (gpapadis@yahoo.gr)
 */
package DataStructures;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

/**
 * Classes obtained from: http://sourceforge.net/projects/erframework/files/CleanCleanERDatasets/
 * Modified
 * @author gap2
 */

public class EntityProfile implements Serializable {

    private static final long serialVersionUID = 122354534453243447L;

    private final Set<Attribute> attributes;
    private String entityUrl;

    public EntityProfile(String url) {
        entityUrl = url;
        attributes = new HashSet();
    }

    public void addAttribute(String propertyName, String propertyValue) {
        attributes.add(new Attribute(propertyName, propertyValue));
    }

    public String getEntityUrl() {
        return entityUrl;
    }

    //replaces url id from entity
    public void setEntityUrl(String newurl) {entityUrl = newurl;}

    public int getProfileSize() {
        return attributes.size();
    }

    public Set<Attribute> getAttributes() {
        return attributes;
    }
}