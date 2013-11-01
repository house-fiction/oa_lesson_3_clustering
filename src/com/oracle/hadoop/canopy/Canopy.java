package com.oracle.hadoop.canopy;



import java.io.IOException;

import java.util.*;
import java.nio.CharBuffer;
import java.nio.ByteBuffer;


public class Canopy
{

    public String [] canopyVector;
    public int canopyId;
    
    public Canopy(int cID, String [] cV)
    {
	canopyId = cID;
	canopyVector = cV;
    }

    public boolean equals(Canopy b)
    {
	if (b.canopyId == this.canopyId)
	    return true;
	return false;
    }

    
}
