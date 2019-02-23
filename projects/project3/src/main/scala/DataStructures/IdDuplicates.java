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

/**
 *
 * @author gap2
 */

public class IdDuplicates implements Serializable {

    private static final long serialVersionUID = 7234234586147L;

    private final int entityId1;
    private final int entityId2;

    public IdDuplicates(int id1, int id2) {
        entityId1 = id1;
        entityId2 = id2;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final IdDuplicates other = (IdDuplicates) obj;
        if (this.entityId1 != other.entityId1) {
            return false;
        }
        if (this.entityId2 != other.entityId2) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 83 * hash + this.entityId1;
        hash = 83 * hash + this.entityId2;
        return hash;
    }

    public int getEntityId1() {
        return entityId1;
    }

    public int getEntityId2() {
        return entityId2;
    }
}