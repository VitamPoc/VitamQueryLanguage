/**
 * This file is part of POC MongoDB ElasticSearch Project.
 *
 * Copyright 2009, Frederic Bregier, and individual contributors by the @author tags. See the
 * COPYRIGHT.txt in the distribution for a full listing of individual contributors.
 *
 * All POC MongoDB ElasticSearch Project is free software: you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free Software Foundation,
 * either version 3 of the License, or (at your option) any later version.
 *
 * POC MongoDB ElasticSearch is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
 * PURPOSE. See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with POC MongoDB
 * ElasticSearch . If not, see <http://www.gnu.org/licenses/>.
 */
package fr.gouv.vitam.mdbes;

import java.io.BufferedOutputStream;
import java.util.Date;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

import org.bson.BSONObject;
import org.bson.types.BasicBSONList;
import org.joda.time.DateTime;

import com.mongodb.BasicDBObject;
import com.mongodb.util.JSON;

import fr.gouv.vitam.mdbes.MongoDbAccess.VitamCollections;
import fr.gouv.vitam.query.exception.InvalidExecOperationException;
import fr.gouv.vitam.query.exception.InvalidParseOperationException;
import fr.gouv.vitam.query.exception.InvalidUuidOperationException;
import fr.gouv.vitam.utils.GlobalDatas;
import fr.gouv.vitam.utils.logging.VitamLogger;
import fr.gouv.vitam.utils.logging.VitamLoggerFactory;

/**
 * <pre>
 * {@code
les champs __XXX ne seront pas stockes dans le resultat arborescent

Valeurs fixes: valeur
=====================
    valeurSimple = "chaine" | nombre
        ou "chaine" = une chaine de caractere fixe
        ou nombre = un nombre fixe
        
    valeur = valeurSimple | listeValeurs
        ou listeValeurs = [ valeurSimple, ... ]

L'expression des champs: champs
===============================
    champ = "nomDuChamp" : valeur | typeSimple | typeComplexe
        => associe a un nom de champ (chaine) une valeur ou un type simple ou un type complexe
    
    champs = champ | champ, champs
        La notation "champ, champ, ..." signifie serie de champs (exemple : { champ1 : val1, champ2 : val2, champ3 : val3 } )

L'expression des occurences d'un niveau de MetaAip (variabilisation) : occurence
================================================================================
    occurence_unique = "__occur" : nombre (> 0)
        => signifie qu'il y aura 'nombre' fils pour un noeud pere donne et le compteur vaudra entre 0 et nombre.

    occurence_partage = "__occur" : nombre (>0) , "__idcpt" : "nomDuCompteur"
        => signifie qu'il y aura 'nombre' fils pour un noeud pere donne MAIS le compteur sera global (unique et partage par son nom dans le champ __idcpt) et variera de 0 a l'infini (par lots de 'nombre')

    occurence_bound = "__occur" : valeur, "__high" : valeur, "__idcpt" : "nomDuCompteur"
        => variera entre low (valeur) et high (inclus)

    Options complémentaires:
    "__distrib" : base => indique le compteur utilisé par la répartition avec pour valeur de base (low) = base
    "__notempty" : 1 => indique que si un objet créé est vide, il ne sera pas conservé (vide si pas de fils ou si getObject = null)
    
    Dans tous les cas, le compteur produit devient le compteur de reference pour les MetaAip crees ensuite pour ce niveau (et uniquement celui-la).

    occurence = occurence_unique | occurence_partage

Types simples: typeSimple
=========================
    { "__type" : "random", "__ftype" : "chaine", "__subprefix" : [ "nomValeur", ... ], "__save" : "nomValeur" } : une chaine de caracteres
        => une chaine aleatoire sera produite

    { "__type" : "random", "__ftype" : "date", "__save" : "nomValeur" } : une date
        => une date aleatoire sera produite

    { "__type" : "random", "__ftype" : "nombre", "__save" : "nomValeur" } : un format numerique (avec ou sans virgule)
        => un nombre aleatoire sera produite
    
    { "__type" : "save", "__subprefix" : [ "nomValeur", ... ], "__save" : "nomValeur" } : une chaine de caracteres
        => une chaine est produite par la concatenation des prefix

Types complexes: typeComplexe
=============================
    { "__type" : "interval", "__low" : valeur, "__high" : valeur, "__subprefix" : [ "nomValeur", ... ], "__save" : "nomValeur" }
        => prend une valeur aléatoire entre low et high (inclus)

    { "__type" : "liste", "__liste" : listeValeurs, "__subprefix" : [ "nomValeur", ... ], "__save" : "nomValeur" }
        => de maniere aleatoire, un element de la liste sera attribue au champ

    { "__type" : "listeorder", "__listeorder" : listeValeurs, "__subprefix" : [ "nomValeur", ... ], "__save" : "nomValeur" }
        => le premier objet du pere commun aura pour valeur la valeur de rang 1, le deuxieme la valeur de rang 2, etc. 
        Si le nombre de fils du pere depasse le nombre de valeurs dans la liste, la derniere valeur est re-utilisee.
        Exemple : { champ : { "__type" : "listeorder", "__listeorder" : [ "A", "B" ] } } avec 5 fils de ce type (occurence._occur = 3)
        => premier fils : { champ : "A" }
        => deuxieme fils : { champ : "B" }
        => troisieme fils : { champ : "B" }

    { "__type" : "serie", "__serie" : { "__prefix" : chaine, "__idcpt" : "nomDuCompteur", "__modulo" : nombre}, "__subprefix" : [ "nomValeur", ... ], "__save" : "nomValeur" }
        => une chaine aleatoire sera produite du type : "prefixe"+Valeur" ou Valeur sera
        __idcpt est precise : la valeur courante du compteur specifie est utilisee
        __idcpt non precise : la valeur courante du compteur de reference est utilisee
        __modulo est precise : la valeur est calcule par nombre = (nombre % modulo)+1
        __modulo n'est pas precise : la valeur est non modifiee
        Exemple : { champ : { "__type" : "serie", "__serie" : { "__prefix" : "Pref_", "__idcpt" : "moncpt", "__modulo" : 3 } } } avec __occur = 4 et moncpt = 10 au depart
        => premier fils : moncpt = 11 => (11 mod 3) +1 = 3 => { champ : "Pref_3" }
        => deuxieme fils : moncpt = 12 => (12 mod 3) +1 = 1 => { champ : "Pref_1" }
        => troisieme fils : moncpt = 13 => (13 mod 3) +1 = 2 => { champ : "Pref_2" }
        => quatrieme fils : moncpt = 14 => (14 mod 3) +1 = 3 => { champ : "Pref_3" }

    { "__type" : "subfield" , "__subfield" : { champs } }
        => un champ compose sera cree (potentiellement complexe)
        Exemple : { "champ1" : { "__type" : "subfield" , "__subfield" : { "champ2" : "valeur" } } }
        => champ1.champ2 = "valeur" => { champ1 : { champ2 : "valeur" } }

    { "__type" : "select", "__select" : [ {"__field" : "name", "__value": ["nomValeur"]}, ... ] }
    
Mots Clefs de structures: domaine
=================================
    domain = { "Domain" : "chaine1", "__model": "chaine2", daip, extraStaticField }
        => definie que ce modele chaine2 appartient a la racine Domaine ayant pour nom "chaine1"
        => definie la sous-structure metaaip (unique) a utiliser
        => pas de champ
    
    daip = "DAip" : [ { occurence }, { champs }, sous-structure ]
        => definie un niveau de MetaAip avec ses champs et sa sous-structure (unique)
        
    paip = { "PAip" : { champs } }
        => definie un DataObject avec ses champs mais il n'aura pas de sous-structure (pour le POC)
        => ses champs devront avoir deux categories : ceux propres au modele, et ceux communs et fixes (exemple : format, empreinte, ...)
    
    sous-structure = { daip } | paip

    
Exemple de definition d'un modele: 
==================================
{ "Domain" : "domainName", "__model" : "modelName",
    "DAip" : [
        { "__occur" : 100}, 
        { "champ1" : "chaine", "champ2" : { "__type" : "liste", "__liste" : [ "val1", "val2" ] } },
        { "DAip" : [
            { "__occur" : 1000, "__idcpt" : "moncpt" },
            { "champ3" : { "__type" : "serie", "__serie" : { "__prefix" : "Pref_", "__idcpt" : "moncpt" } } },
            { "DAip" : [
                { "__occur" : 10 },
                { "champ4" : { "__type" : "listeorder", "__listeorder" : [ "val3", "val4" ] },
                  "champ5" : { "__type" : "serie", "__serie" : { "__prefix" : "do_" } } },
                { "PAip" : { "champ6" : "chaine" } }
                ]
            } ]
        } ]
}

=>
{ Domain : "chaine",
    DAip : [ 
        { champ1 : "valeur", champ2 : "val2",
        DAip : [
            { champ3 : "Pref_1", 
            DAip : [
                { champ4 : "val3", champ5 : "do_1", PAip : { champ6 : "val6" } },
                { champ4 : "val4", champ5 : "do_2", PAip : { champ6 : "val6" } },
                { champ4 : "val4", champ5 : "do_3", PAip : { champ6 : "val6" } },
                ...
                { champ4 : "val4", champ5 : "do_10", PAip : { champ6 : "val6" } },
            ] },
            { champ3 : "Pref_2", 
            DAip : [
                { champ4 : "val3", champ5 : "do_1", PAip : { champ6 : "val6" } },
                { champ4 : "val4", champ5 : "do_2", PAip : { champ6 : "val6" } },
                ...
                { champ4 : "val4", champ5 : "do_10", PAip : { champ6 : "val6" } },
            ] },
            ...
            { champ3 : "Pref_1000", 
            DAip : [
                { champ4 : "val3", champ5 : "do_1", PAip : { champ6 : "val6" } },
                { champ4 : "val4", champ5 : "do_2", PAip : { champ6 : "val6" } },
                ...
                { champ4 : "val4", champ5 : "do_10", PAip : { champ6 : "val6" } },
            ] }
        ] },
        { champ1 : "valeur", champ2 : "val1",
        DAip : [
            { champ3 : "Pref_1001", 
            DAip : [
                { champ4 : "val3", champ5 : "do_1", PAip : { champ6 : "val6" } },
                { champ4 : "val4", champ5 : "do_2", PAip : { champ6 : "val6" } },
                { champ4 : "val4", champ5 : "do_3", PAip : { champ6 : "val6" } },
                ...
                { champ4 : "val4", champ5 : "do_10", PAip : { champ6 : "val6" } },
            ] },
            { champ3 : "Pref_1002", 
            DAip : [
                { champ4 : "val3", champ5 : "do_1", PAip : { champ6 : "val6" } },
                { champ4 : "val4", champ5 : "do_2", PAip : { champ6 : "val6" } },
                ...
                { champ4 : "val4", champ5 : "do_10", PAip : { champ6 : "val6" } },
            ] },
            ...
            { champ3 : "Pref_2000", 
            DAip : [
                { champ4 : "val3", champ5 : "do_1", PAip : { champ6 : "val6" } },
                { champ4 : "val4", champ5 : "do_2", PAip : { champ6 : "val6" } },
                ...
                { champ4 : "val4", champ5 : "do_10", PAip : { champ6 : "val6" } },
            ] }
        ] },
        ...
    ]
}

}</pre>
 * @author "Frederic Bregier"
 *
 */
@SuppressWarnings("javadoc")
public class ParserIngest {
    private static final VitamLogger LOGGER = VitamLoggerFactory.getInstance(ParserIngest.class);

    protected BufferedOutputStream bufferedOutputStream = null;
    private static final String CPTLEVEL = "__cptlevel__";
    private static final String REFID = "_refid";

    private static enum OCCURENCE_ARGS {
        __occur, __idcpt, __high, __distrib, __notempty
    }
    
    private static enum FIELD_TYPE {
        chaine, date, nombre, nombrevirgule
    }

    private static enum FIELD {
        random, save, constant, constantArray, liste, listeorder, serie, subfield, interval, select
    }

    private static enum FIELD_ARGS {
        __model, __idcpt, __type, __ftype, __liste, __listeorder, __serie, __prefix, __modulo, __subprefix, __low, __high, __field, __value, __save, __subfield
    }

    private static class Occurence {
        private int occur;
        private int low;
        private int high;
        private int distrib;
        private boolean notempty = false;
        private String idcpt;
    }

    private static class TypeField {
        private String name;
        /**
         * Between random, liste, listeorder, serie, subfield, constant, constantArray
         */
        private FIELD type;
        /**
         * Type of field (chaine, date, nombre, nombrevirgule), default being chaine
         */
        private FIELD_TYPE ftype = FIELD_TYPE.chaine;
        /**
         * Used in liste, listeorder, constantArray
         */
        private String[] listeValeurs;
        /**
         * Used in serie and prefix for fixed value in constant
         */
        private String prefix;
        private String idcpt;
        private int modulo;
        /**
         * Interval
         */
        private int low;
        private int high;
        /**
         * saved as new current value for name
         */
        private String saveName;
        /**
         * add several prefix to value
         */
        private String[] subprefixes;
        /**
         * Used in subfield
         */
        private List<TypeField> subfields;

        @Override
        public String toString() {
            final StringBuilder builder = new StringBuilder();
            builder.append("\nField: ");
            builder.append(name);
            builder.append(" Type: " + type);
            builder.append(" FType: " + ftype);
            builder.append(" Prefix: " + prefix);
            builder.append(" Cpt: " + idcpt);
            builder.append(" Modulo: " + modulo);
            builder.append(" High: " + high);
            builder.append(" Save: " + saveName);
            if (listeValeurs != null) {
                builder.append(" Vals: " + listeValeurs.length);
            }
            builder.append(" Save: " + saveName);
            if (subprefixes != null) {
                builder.append(" SubPrefix: [ ");
                for (final String curname : subprefixes) {
                    builder.append(curname);
                    builder.append(' ');
                }
                builder.append(']');
            }
            if (subfields != null) {
                builder.append(" Subfields: " + subfields.size());
                builder.append("\n\t[");
                for (final TypeField typeSubField : subfields) {
                    builder.append(typeSubField.toString());
                }
                builder.append("\n\t]");
            }
            return builder.toString();
        }
    }

    private String ingest;
    private MongoDbAccess dbvitam;
    private String domRefid;
    private BSONObject domObj;
    private Domain domobj;
    private String model;
    private Occurence distribOccurence = null;

    private final BenchContext context = new BenchContext();
    private final ArrayList<Occurence> occurences = new ArrayList<>();
    /**
     * Model of DAIPs from model
     */
    private final ArrayList<List<TypeField>> daips = new ArrayList<>();
    /**
     * Model of PAip from model
     */
    private List<TypeField> paips = null;

    private final AtomicLong totalCount = new AtomicLong(0);

    private boolean simulate = false;
    
    protected static Map<String, DAip> savedDaips = new HashMap<String, DAip>();

    public ParserIngest(final boolean simul) throws InvalidParseOperationException {
        setSimulate(simul);
    }

    /**
     * @return the totalCount
     */
    public long getTotalCount() {
        return totalCount.get();
    }

    public void setSimulate(final boolean simul) {
        simulate = simul;
    }

    private void internalParseClean() {
        context.cpts.clear();
        context.savedNames.clear();
        occurences.clear();
        daips.clear();
        if (paips != null) {
            paips.clear();
        }
    }

    /**
     * Parse the model of ingest from the string
     *
     * @param ingest
     * @throws InvalidParseOperationException
     */
    public void parse(final String ingest) throws InvalidParseOperationException {
        internalParseClean();
        this.ingest = ingest;
        final BSONObject bson = (BSONObject) JSON.parse(ingest);
        model = (String) bson.removeField(FIELD_ARGS.__model.name());
        getDomain(bson);
        parseDAip(bson, 1);
        domObj = bson;
    }

    public String getModel() {
        return model;
    }

    @Override
    public String toString() {
        final StringBuilder builder = new StringBuilder();
        builder.append("Model: ");
        builder.append(model);
        builder.append(" Domaine: ");
        builder.append(domRefid);
        if (domObj != null && !domObj.toMap().isEmpty()) {
            builder.append(" other: " + domObj.toString());
        }
        final int nb = occurences.size();
        for (int i = 0; i < nb; i++) {
            builder.append("\nMaipLevel: ");
            builder.append((i + 1));
            final Occurence occur = occurences.get(i);
            builder.append(" Occurence: ");
            builder.append(occur.occur + ":" + occur.idcpt + ":" + occur.low + ":" + occur.high + ":" + occur.notempty);
            final List<TypeField> fields = daips.get(i);
            for (final TypeField typeField : fields) {
                builder.append(typeField.toString());
            }
        }
        builder.append("\nDataLevel: ");
        builder.append((nb + 1));
        if (paips != null) {
            for (final TypeField typeField : paips) {
                builder.append(typeField.toString());
            }
        }
        return builder.toString();
    }

    /**
     * Set the domain Refid from model
     *
     * @param bson
     * @throws InvalidParseOperationException
     */
    private void getDomain(final BSONObject bson) throws InvalidParseOperationException {
        // Find Domaine as reference (and possibly create later)
        domRefid = (String) bson.removeField(MongoDbAccess.VitamCollections.Cdomain.getName());
    }

    /**
     * Parse the bson model from level
     *
     * @param bson
     * @param level
     *            current level
     * @throws InvalidParseOperationException
     */
    private void parseDAip(final BSONObject bson, final int level) throws InvalidParseOperationException {
        if (bson.containsField(MongoDbAccess.VitamCollections.Cdaip.getName())) {
            final BasicBSONList metaaips = (BasicBSONList) bson.removeField(MongoDbAccess.VitamCollections.Cdaip.getName());
            // should be 0 = occurence, 1 = fields, 2 = sub-structure (maip or dataobject)
            final BSONObject occur = (BSONObject) metaaips.get(0);
            final BSONObject maipModel = (BSONObject) metaaips.get(1);
            final BSONObject substruct = (BSONObject) metaaips.get(2);
            final int nb = (int) occur.get(OCCURENCE_ARGS.__occur.name());
            final Occurence oc = new Occurence();
            oc.occur = nb;
            if (occur.containsField(OCCURENCE_ARGS.__high.name())) {
                oc.high = (Integer) occur.get(OCCURENCE_ARGS.__high.name());
                oc.low = nb;
                oc.occur = oc.high - oc.low + 1;
                oc.distrib = oc.low;
            } else {
                oc.low = 0;
                oc.high = nb - 1;
                oc.distrib = oc.low;
            }
            if (occur.containsField(OCCURENCE_ARGS.__distrib.name())) {
                oc.distrib = (Integer) occur.get(OCCURENCE_ARGS.__distrib.name());
                distribOccurence = oc;
                if (GlobalDatas.PRINT_REQUEST) {
                    System.out.println("Found a distribution occurence");
                }
            }
            if (occur.containsField(OCCURENCE_ARGS.__idcpt.name())) {
                oc.idcpt = (String) occur.get(OCCURENCE_ARGS.__idcpt.name());
                context.cpts.put(oc.idcpt, new AtomicLong(oc.low));
            }
            if (occur.containsField(OCCURENCE_ARGS.__notempty.name())) {
                oc.notempty = true;
            }
            context.cpts.put(CPTLEVEL + level, new AtomicLong(oc.low));
            occurences.add(oc);
            // now parse MetaAip field
            final List<TypeField> fields = new ArrayList<>();
            if (GlobalDatas.PRINT_REQUEST) {
                System.out.println("LoadM: " + level + " " + maipModel);
            }
            final Set<String> fieldnames = maipModel.keySet();
            for (final String fieldname : fieldnames) {
                final Object bfield = maipModel.get(fieldname);
                if (bfield instanceof String) {
                    // single value
                    final TypeField field = new TypeField();
                    field.name = fieldname;
                    field.prefix = (String) bfield;
                    field.type = FIELD.constant;
                    fields.add(field);
                } else if (bfield instanceof BasicBSONList) {
                    // single value
                    final BasicBSONList blist = (BasicBSONList) bfield;
                    final TypeField field = new TypeField();
                    field.name = fieldname;
                    field.listeValeurs = new String[blist.size()];
                    for (int j = 0; j < blist.size(); j++) {
                        field.listeValeurs[j] = (String) blist.get(j);
                    }
                    field.type = FIELD.constantArray;
                    fields.add(field);
                } else {
                    final TypeField field = getField((BSONObject) bfield);
                    field.name = fieldname;
                    fields.add(field);
                }
            }
            daips.add(fields);
            parseDAip(substruct, level + 1);
        } else if (bson.containsField(MongoDbAccess.VitamCollections.Cpaip.getName())) {
            parsePAip(bson, level);
        }
    }

    /**
     *
     * @param bfield
     * @return one TypeField according to bson element
     * @throws InvalidParseOperationException
     */
    private TypeField getField(final BSONObject bfield) throws InvalidParseOperationException {
        final String type = (String) bfield.get(FIELD_ARGS.__type.name());
        if (type == null || type.isEmpty()) {
            LOGGER.error("Unknown empty type: " + type);
            throw new InvalidParseOperationException("Unknown empty type: " + type);
        }
        final TypeField field = new TypeField();
        FIELD fieldType = null;
        try {
            fieldType = FIELD.valueOf(type);
        } catch (final IllegalArgumentException e) {
            LOGGER.error("Unknown type: " + bfield);
            throw new InvalidParseOperationException("Unknown type: " + bfield);
        }
        field.type = fieldType;
        final String sftype = (String) bfield.get(FIELD_ARGS.__ftype.name());
        FIELD_TYPE ftype = FIELD_TYPE.chaine;
        if (sftype != null && ! sftype.isEmpty()) {
            try {
                ftype = FIELD_TYPE.valueOf(sftype);
            } catch (final IllegalArgumentException e) {
                LOGGER.error("Unknown ftype: " + bfield);
                ftype = FIELD_TYPE.chaine;
            }
        }
        field.ftype = ftype;
        switch (fieldType) {
            case random:
            case save:
                break;
            case liste:
            case listeorder: {
                final BasicBSONList liste = (BasicBSONList) bfield.get("__" + fieldType.name());
                if (liste == null || liste.isEmpty()) {
                    LOGGER.error("Empty List: " + liste);
                    throw new InvalidParseOperationException("Empty List: " + bfield);
                }
                field.listeValeurs = new String[liste.size()];
                for (int i = 0; i < liste.size(); i++) {
                    field.listeValeurs[i] = (String) liste.get(i);
                }
                break;
            }
            case serie: {
                final BSONObject bson = (BSONObject) bfield.get(FIELD_ARGS.__serie.name());
                if (bson == null) {
                    LOGGER.error("Empty serie: " + bfield);
                    throw new InvalidParseOperationException("Empty serie: " + bfield);
                }
                if (bson.containsField(FIELD_ARGS.__prefix.name())) {
                    String prefix = (String) bson.get(FIELD_ARGS.__prefix.name());
                    if (prefix == null) {
                        prefix = "";
                    }
                    field.prefix = prefix;
                }
                if (bson.containsField(FIELD_ARGS.__idcpt.name())) {
                    final String idcpt = (String) bson.get(FIELD_ARGS.__idcpt.name());
                    if (idcpt != null && !idcpt.isEmpty()) {
                        field.idcpt = idcpt;
                    }
                }
                field.modulo = -1;
                if (bson.containsField(FIELD_ARGS.__modulo.name())) {
                    final int modulo = (int) bson.get(FIELD_ARGS.__modulo.name());
                    if (modulo > 0) {
                        field.modulo = modulo;
                    }
                }
                break;
            }
            case subfield: {
                final BSONObject subfield = (BSONObject) bfield.get(FIELD_ARGS.__subfield.name());
                if (subfield == null) {
                    LOGGER.error("Unknown subfield: " + bfield);
                    throw new InvalidParseOperationException("Unknown subfield: " + bfield);
                }
                final List<TypeField> fields = new ArrayList<>();
                if (GlobalDatas.PRINT_REQUEST) {
                    System.out.println("LoadMS: " + subfield);
                }
                final Set<String> fieldnames = subfield.keySet();
                for (final String fieldname : fieldnames) {
                    final BSONObject bfield2 = (BSONObject) subfield.get(fieldname);
                    final TypeField field2 = getField(bfield2);
                    field2.name = fieldname;
                    fields.add(field2);
                }
                field.subfields = fields;
                break;
            }
            case interval: {
                final Integer low = (Integer) bfield.get(FIELD_ARGS.__low.name());
                if (low == null) {
                    LOGGER.error("Empty interval: " + bfield);
                    throw new InvalidParseOperationException("Empty interval: " + bfield);
                }
                final Integer high = (Integer) bfield.get(FIELD_ARGS.__high.name());
                if (high == null) {
                    LOGGER.error("Empty interval: " + bfield);
                    throw new InvalidParseOperationException("Empty interval: " + bfield);
                }
                field.low = low;
                field.high = high;
                break;
            }
            case select: {
                final BasicBSONList liste = (BasicBSONList) bfield.get("__" + fieldType.name());
                if (liste == null || liste.isEmpty()) {
                    LOGGER.error("Empty List: " + liste);
                    throw new InvalidParseOperationException("Empty List: " + bfield);
                }
                field.subfields = new ArrayList<>();
                for (int i = 0; i < liste.size(); i++) {
                    final TypeField request = new TypeField();
                    final BSONObject obj = (BSONObject) liste.get(i);
                    request.name = (String) obj.get(FIELD_ARGS.__field.name());
                    final BasicBSONList prefixes = (BasicBSONList) obj.get(FIELD_ARGS.__value.name());
                    if (prefixes == null || prefixes.isEmpty()) {
                        LOGGER.error("Empty prefixes: " + prefixes);
                        throw new InvalidParseOperationException("Empty prefixes: " + obj);
                    }
                    request.subprefixes = new String[prefixes.size()];
                    for (int j = 0; j < prefixes.size(); j++) {
                        request.subprefixes[j] = (String) prefixes.get(j);
                    }
                    field.subfields.add(request);
                }
                break;
            }
            default:
                LOGGER.error("Incorrect type: " + bfield);
                throw new InvalidParseOperationException("Incorrect type: " + bfield);
        }
        if (bfield.containsField(FIELD_ARGS.__save.name())) {
            final String savename = (String) bfield.get(FIELD_ARGS.__save.name());
            if (savename != null && !savename.isEmpty()) {
                field.saveName = savename;
            }
        }
        if (bfield.containsField(FIELD_ARGS.__subprefix.name())) {
            final BasicBSONList liste = (BasicBSONList) bfield.get(FIELD_ARGS.__subprefix.name());
            if (liste == null || liste.isEmpty()) {
                LOGGER.error("Empty SubPrefix List: " + liste);
                throw new InvalidParseOperationException("Empty SubPrefix List: " + bfield);
            }
            field.subprefixes = new String[liste.size()];
            for (int i = 0; i < liste.size(); i++) {
                field.subprefixes[i] = (String) liste.get(i);
            }
        }
        return field;
    }

    /**
     * Parse the final PAip
     *
     * @param bson
     * @param level
     * @throws InvalidParseOperationException
     */
    private void parsePAip(final BSONObject bson, final int level) throws InvalidParseOperationException {
        if (bson.containsField(MongoDbAccess.VitamCollections.Cpaip.getName())) {
            final BSONObject bdata = (BSONObject) bson.removeField(MongoDbAccess.VitamCollections.Cpaip.getName());
            context.cpts.put(CPTLEVEL + level, new AtomicLong(0));
            final List<TypeField> fields = new ArrayList<>();
            if (GlobalDatas.PRINT_REQUEST) {
                System.out.println("LoadD: " + level + " " + bdata);
            }
            final Set<String> fieldnames = bdata.keySet();
            for (final String fieldname : fieldnames) {
                final Object bfield = bdata.get(fieldname);
                if (bfield instanceof String) {
                    // single value
                    final TypeField field = new TypeField();
                    field.name = fieldname;
                    field.prefix = (String) bfield;
                    field.type = FIELD.constant;
                    fields.add(field);
                } else if (bfield instanceof BasicBSONList) {
                    // single value
                    final BasicBSONList blist = (BasicBSONList) bfield;
                    final TypeField field = new TypeField();
                    field.name = fieldname;
                    field.listeValeurs = new String[blist.size()];
                    for (int j = 0; j < blist.size(); j++) {
                        field.listeValeurs[j] = (String) blist.get(j);
                    }
                    field.type = FIELD.constantArray;
                    fields.add(field);
                } else {
                    final TypeField field = getField((BSONObject) bfield);
                    field.name = fieldname;
                    fields.add(field);
                }
            }
            paips = fields;
        }
    }

    /**
     * Save generated DAip to the file and to ElasticSearch
     *
     * @param dbvitam
     * @param start
     * @param stop
     * @param saveEs True means save to ES
     * @return the number of element inserted
     * @throws InvalidExecOperationException
     * @throws InvalidUuidOperationException
     */
    public long executeToFile(final MongoDbAccess dbvitam, final int start, final int stop, final boolean saveEs) throws InvalidExecOperationException,
    InvalidUuidOperationException {
        if (simulate) {
            return executeSimulate(start, stop);
        }
        if (GlobalDatas.PRINT_REQUEST) {
            System.out.println("Start To File");
        }
        this.dbvitam = dbvitam;
        // Domain
        domobj = (Domain) dbvitam.fineOne(VitamCollections.Cdomain, REFID, domRefid);
        LOGGER.debug("Found Domain ? " + (domobj != null));
        if (domobj == null) {
            domobj = new Domain();
            domobj.put(REFID, domRefid);
            domobj.put("name", domRefid);
            domobj.putAll(domObj);
            domobj.save(dbvitam);
            domobj.setRoot();
            LOGGER.warn("Create Domain: {}", domobj);
            // LOGGER.error("Load: "+domobj);
        }
        // Set DISTRIB to start-stop
        if (distribOccurence != null) {
            final int lstop = (stop - start + 1 > distribOccurence.occur) ? start + distribOccurence.occur - 1 : stop;
            distribOccurence.low = start;
            distribOccurence.high = lstop;
            distribOccurence.occur = lstop - start + 1;
            System.out.println("Distrib: " + start + ":" + lstop);
        }
        // First level using start-stop
        AtomicLong cpt = context.cpts.get(CPTLEVEL + 1);
        final List<TypeField> fields = daips.get(0);
        final Occurence occurence = occurences.get(0);
        cpt.set(occurence.low);
        if (occurence.idcpt != null) {
            cpt = context.cpts.get(occurence.idcpt);
        }
        final long lstart = cpt.get();
        final long lstop = lstart + occurence.occur - 1;
        ArrayList<DAip> listmetaaips;
        final HashMap<String, Integer> subdepth = new HashMap<>();
        subdepth.put(domobj.getId(), 1);
        HashMap<String, String> esIndex = null;
        if (saveEs) {
            esIndex = new HashMap<>();            
        }
        try {
            listmetaaips = execDAipNewToFile(null, subdepth, esIndex, 1, occurence, lstart, lstop, cpt, fields);
        } catch (InstantiationException | IllegalAccessException e) {
            throw new InvalidExecOperationException(e);
        } catch (Exception e) {
            throw new InvalidExecOperationException(e);
        }
        if (GlobalDatas.PRINT_REQUEST) {
            System.out.println("End of MAIPs");
        }
        if (listmetaaips != null && !listmetaaips.isEmpty()) {
            if (MainIngestFile.minleveltofile > 1) {
                domobj.addDAip(dbvitam, listmetaaips);
                System.out.println("To be saved: "+listmetaaips.size());
                for (DAip dAip : listmetaaips) {
                    savedDaips.put(dAip.getId(), dAip);
                }
            } else {
                // XXX NO SAVE OF MAIP!
                domobj.addDAipNoSave(dbvitam, bufferedOutputStream, listmetaaips);
                if (saveEs) {
                    for (DAip dAip : listmetaaips) {
                        final BSONObject bson = (BSONObject) JSON.parse(dAip.toStringDirect());
                        ElasticSearchAccess.addEsIndex(dbvitam, model, esIndex, bson);
                    }
                }
            }
            domobj.save(dbvitam);
            listmetaaips.clear();
            listmetaaips = null;
        }
        if (saveEs && !esIndex.isEmpty()) {
            System.out.println("Last bulk ES");
            dbvitam.addEsEntryIndex(true, esIndex, model);
            esIndex.clear();
        }
        System.out.println("End of Domain");
        return totalCount.get();
    }

    /**
     * Level 1 to MD, not > 1
     *
     * Save to file and to ElasticSearch
     *
     * @param father
     * @param subdepth22
     * @param esIndex
     * @param level
     * @param occurence
     * @param lstart
     * @param lstop
     * @param cpt
     * @param fields
     * @return the list of immediate sons
     * @throws InvalidExecOperationException
     * @throws InstantiationException
     * @throws IllegalAccessException
     */
    private ArrayList<DAip> execDAipNewToFile(final DAip father, final Map<String, Integer> subdepth22,
            final HashMap<String, String> esIndex, final int level, final Occurence occurence, final long lstart,
            final long lstop, final AtomicLong cpt, final List<TypeField> fields) throws InvalidExecOperationException,
            InstantiationException, IllegalAccessException {
        final ArrayList<DAip> listmetaaips = new ArrayList<DAip>();
        final boolean fromDatabase = level < MainIngestFile.minleveltofile;
        for (long rank = cpt.get(); rank <= lstop; rank = cpt.incrementAndGet()) {
            DAip maip = new DAip();
            maip.put(DAip.DAIPDEPTHS, subdepth22);
            for (final TypeField typeField : fields) {
                final BasicDBObject obj = getDbObject(typeField, rank, occurence.distrib, cpt);
                if (obj == null) {
                    maip = null;
                    break;
                }
                maip.putAll((BSONObject) obj);
            }
            if (maip == null) {
                continue;
            }
            totalCount.incrementAndGet();
            if (totalCount.get() % 1000 == 0) {
                System.out.print('.');
            }
            if (occurence == distribOccurence) {
                System.out.println("\nDistrib: " + rank);
            }
            maip.getAfterLoad();
            DAip metaaip2 = null;
            if (fromDatabase) {
                metaaip2 = (DAip) dbvitam.fineOne(VitamCollections.Cdaip, REFID, maip.getString(REFID));
            }
            boolean metaCreated = true;
            if (metaaip2 != null) {
                System.out.print('x');
                // merge Depth
                final Map<String, Integer> old = metaaip2.getDomDepth();
                // Map<String, Integer> toUpdateSon = new HashMap<String, Integer>();
                for (final String key : subdepth22.keySet()) {
                    if (old.containsKey(key)) {
                        if (old.get(key) > subdepth22.get(key)) {
                            old.put(key, subdepth22.get(key));
                            // toUpdateSon.put(key, subdepth22.get(key));
                        }
                    } else {
                        old.put(key, subdepth22.get(key));
                        // toUpdateSon.put(key, subdepth22.get(key));
                    }
                }
                // old now contains all
                metaaip2.put(DAip.DAIPDEPTHS, old);
                // XXX FIXME should update children but will not since "POC" code and not final code
                // XXX FIXME here should do: recursive call to update DAIPDEPTHS from toUpdateSon
                maip = metaaip2;
                // System.out.println("Not created: "+metaaip2.toString());
                metaCreated = false;
                // Last level
                if (level >= daips.size()) {
                    // update directly
                    if (father == null) {
                        listmetaaips.add(metaaip2);
                    }
                    metaaip2.save(dbvitam);
                    if (esIndex != null) {
                        final BSONObject bson = (BSONObject) JSON.parse(maip.toStringDirect());
                        ElasticSearchAccess.addEsIndex(dbvitam, model, esIndex, bson);
                    }
                    continue;
                }
            }
            if (metaCreated) {
                // since created saved once here
                // XXX NO SAVE OF MAIP!
                maip.setNewId();
                // now check duaref and confidentialLevel
                if (maip.containsField(MongoDbAccess.VitamCollections.Cdua.getName())) {
                    final String duaname = (String) maip.removeField(MongoDbAccess.VitamCollections.Cdua.getName());
                    final DuaRef duaobj = DuaRef.findOne(dbvitam, duaname);
                    if (duaobj != null) {
                        maip.addDuaRef(dbvitam, duaobj);
                    } else {
                        LOGGER.error("wrong dua: " + duaname);
                    }
                }
            }
            // now compute subMaip if any
            if (level < daips.size()) {
                AtomicLong newcpt = context.cpts.get(CPTLEVEL + (level + 1));
                final List<TypeField> newfields = daips.get(level);
                final Occurence occurence2 = occurences.get(level);
                // default reset
                newcpt.set(occurence2.low);
                if (occurence2.idcpt != null) {
                    newcpt = context.cpts.get(occurence2.idcpt);
                }
                final long newlstart = newcpt.get();
                final long newlstop = newlstart + occurence2.occur - 1;
                final Map<String, Integer> subdepth2 = maip.getSubDomDepth();
                ArrayList<DAip> listsubmetaaips = execDAipNewToFile(maip, subdepth2, esIndex, level + 1, occurence2, newlstart,
                        newlstop, newcpt, newfields);
                if (listsubmetaaips != null) {
                    listsubmetaaips.clear();
                    listsubmetaaips = null;
                }
            } else {
                // now check data
                if (paips != null) {
                    // ignore ? XXX FIXME
                    /*
                     * DataObject dataobj = new DataObject();
                     * for (TypeField typeField : dataObject) {
                     * BasicDBObject obj = getDbObject(typeField, rank, occurence.distrib, cpt);
                     * dataobj.putAll((BSONObject) obj);
                     * }
                     * dataobj.setRefid(maip.refid);
                     * dataobj.getAfterLoad();
                     * dataobj.save(dbvitam);
                     * maip.addDataObject(dbvitam, dataobj);
                     */
                }
            }
            if (father != null) {
                long nb = father.nb;
                father.addDAipWithNoSave(maip);
                if (GlobalDatas.PRINT_REQUEST) {
                    if (level == MainIngestFile.minleveltofile) {
                        System.out.print("Add Daip: "+nb+":"+father.nb);
                    }
                }
            }
            if (fromDatabase) {
                maip.save(dbvitam);
                if (metaCreated) {
                    maip.forceSave(dbvitam.daips);
                }
                savedDaips.put(maip.getId(), maip);
            }
            // System.out.println("M: "+maip.toString());
            if (metaCreated && father == null) {
                listmetaaips.add(maip);
            } else if (father != null && ! fromDatabase) {
                maip.saveToFile(dbvitam, bufferedOutputStream);
                if (esIndex != null) {
                    final BSONObject bson = (BSONObject) JSON.parse(maip.toStringDirect());
                    ElasticSearchAccess.addEsIndex(dbvitam, model, esIndex, bson);
                }
            }
        }
        return listmetaaips;
    }

    /**
     * Execute in simulate mode
     *
     * @param start
     * @param stop
     * @return the number of simulated node
     * @throws InvalidExecOperationException
     */
    public long executeSimulate(final int start, final int stop) throws InvalidExecOperationException {
        simulate = true;
        // Domain
        System.out.println("Domain: " + domRefid);
        // Set DISTRIB to start-stop
        if (distribOccurence != null) {
            System.out.println("Update a distribution occurence from: " + start + ":" + stop);
            final int lstop = (stop - start + 1 > distribOccurence.occur) ? start + distribOccurence.occur - 1 : stop;
            distribOccurence.low = start;
            distribOccurence.high = lstop;
            distribOccurence.occur = lstop - start + 1;
        } else {
            System.out.println("No Found distribution occurence");
        }
        // First level using start-stop
        AtomicLong cpt = context.cpts.get(CPTLEVEL + 1);
        final List<TypeField> fields = daips.get(0);
        final Occurence occurence = occurences.get(0);
        cpt.set(occurence.low);
        System.out.println("CPT: " + (CPTLEVEL + 1) + " " + cpt.get());
        if (occurence.idcpt != null) {
            cpt = context.cpts.get(occurence.idcpt);
            System.out.println("CPT replaced by: " + occurence.idcpt);
        }
        final long lstart = cpt.get();
        final long lstop = lstart + occurence.occur - 1;
        ArrayList<DAip> listmetaaips;
        try {
            listmetaaips = execSimulDAip(1, occurence.distrib, lstart, lstop, cpt, fields);
            System.out.println("SubMaips: " + listmetaaips.size());
        } catch (InstantiationException | IllegalAccessException e) {
            throw new InvalidExecOperationException(e);
        }
        return totalCount.get();
    }

    /**
     * Submethod in simulation mode
     *
     * @param level
     * @param base
     * @param lstart
     * @param lstop
     * @param cpt
     * @param fields
     * @return the sublist of DAips
     * @throws InvalidExecOperationException
     * @throws InstantiationException
     * @throws IllegalAccessException
     */
    private ArrayList<DAip> execSimulDAip(final int level, final int base, final long lstart, final long lstop,
            final AtomicLong cpt, final List<TypeField> fields) throws InvalidExecOperationException, InstantiationException,
            IllegalAccessException {
        final ArrayList<DAip> listmetaaips = new ArrayList<DAip>();
        for (long rank = cpt.get(); rank <= lstop; rank = cpt.incrementAndGet()) {
            DAip maip = new DAip();
            for (final TypeField typeField : fields) {
                final BasicDBObject obj = getDbObject(typeField, rank, base, cpt);
                if (obj == null) {
                    maip = null;
                    break;
                }
                maip.putAll((BSONObject) obj);
            }
            if (maip == null) {
                continue;
            }
            totalCount.incrementAndGet();
            maip.getAfterLoad();
            final boolean metaCreated = true;
            if (metaCreated) {
                // now check duaref and confidentialLevel
                if (maip.containsField(MongoDbAccess.VitamCollections.Cdua.getName())) {
                    maip.get(MongoDbAccess.VitamCollections.Cdua.getName());
                }
            }
            // now compute subMaip if any
            if (level < daips.size()) {
                AtomicLong newcpt = context.cpts.get(CPTLEVEL + (level + 1));
                System.out.println("CPT: " + CPTLEVEL + (level + 1));
                final List<TypeField> newfields = daips.get(level);
                final Occurence occurence = occurences.get(level);
                // default reset
                newcpt.set(occurence.low);
                if (occurence.idcpt != null) {
                    newcpt = context.cpts.get(occurence.idcpt);
                    System.out.println("CPT replaced by: " + occurence.idcpt);
                }
                final long newlstart = newcpt.get();
                final long newlstop = newlstart + occurence.occur - 1;
                ArrayList<DAip> listsubmetaaips = execSimulDAip(level + 1, occurence.distrib, newlstart, newlstop, newcpt,
                        newfields);
                System.out.println("SubMaips: " + listsubmetaaips.size());
                listsubmetaaips.clear();
                listsubmetaaips = null;
            } else {
                // now check data
                if (paips != null) {
                    final PAip dataobj = new PAip();
                    for (final TypeField typeField : paips) {
                        final BasicDBObject obj = getDbObject(typeField, rank, base, cpt);
                        dataobj.putAll((BSONObject) obj);
                    }
                    dataobj.put(REFID, maip.get(REFID));
                    dataobj.getAfterLoad();
                    System.out.println("New Data: " + level + " " + dataobj.toString());
                }
            }
            System.out.println("New Maip: " + level + " " + maip.toString());
            if (metaCreated) {
                listmetaaips.add(maip);
            }
        }
        return listmetaaips;
    }

    /**
     *
     * @param typeField
     * @param curval
     * @return a value for the current field
     */
    private String getValue(final TypeField typeField, final String curval) {
        String finalVal = curval;
        if (typeField.subprefixes != null) {
            String prefix = "";
            for (final String name : typeField.subprefixes) {
                final String tempval = context.savedNames.get(name);
                if (tempval != null) {
                    prefix += tempval;
                } else {
                    prefix += name;
                }
            }
            finalVal = prefix + finalVal;
        }
        if (typeField.saveName != null && finalVal != null) {
            context.savedNames.put(typeField.saveName, finalVal);
        }
        return finalVal;
    }

    private final void setValue(BasicDBObject subobj, TypeField typeField, String value) {
        try {
            switch (typeField.ftype) {
                case date:
                    DateTime dt = new DateTime(value);
                    Date date = new Date(dt.getMillis());
                    subobj.put(typeField.name, date);
                    break;
                case nombre:
                    subobj.put(typeField.name, Long.parseLong(value));
                    break;
                case nombrevirgule:
                    subobj.put(typeField.name, Double.parseDouble(value));
                    break;
                case chaine:
                default:
                    subobj.put(typeField.name, value);
                    break;
            }
        } catch (Exception e) {
            LOGGER.error("Issue with "+typeField+" and value: '"+value+"'", e);
            throw e;
        }
    }
    /**
     *
     * @param typeField
     * @param rank
     * @param lstart
     * @param cpt
     * @return the equivalent BSon object for the current Field
     * @throws InvalidExecOperationException
     */
    private BasicDBObject getDbObject(final TypeField typeField, final long rank, final long lstart, final AtomicLong cpt)
            throws InvalidExecOperationException {
        final BasicDBObject subobj = new BasicDBObject();
        String val = null;
        switch (typeField.type) {
            case constant:
                setValue(subobj, typeField, typeField.prefix);
                break;
            case constantArray:
                subobj.put(typeField.name, typeField.listeValeurs);
                break;
            case random:
                switch (typeField.ftype) {
                    case date:
                        final long curdate = System.currentTimeMillis();
                        final Date date = new Date(curdate - rnd.nextInt());
                        final DateTime dateTime = new DateTime(date);
                        if (typeField.saveName != null) {
                            context.savedNames.put(typeField.saveName, dateTime.toString());
                        }
                        subobj.put(typeField.name, date);
                        break;
                    case nombre:
                        final Long lnval = rnd.nextLong();
                        if (typeField.saveName != null) {
                            context.savedNames.put(typeField.saveName, lnval.toString());
                        }
                        subobj.put(typeField.name, lnval);
                        break;
                    case nombrevirgule:
                        final Double dnval = rnd.nextDouble();
                        if (typeField.saveName != null) {
                            context.savedNames.put(typeField.saveName, dnval.toString());
                        }
                        subobj.put(typeField.name, dnval);
                        break;
                    case chaine:
                    default:
                        val = randomString(32);
                        val = getValue(typeField, val);
                        subobj.put(typeField.name, val);
                        break;
                }
                break;
            case save:
                val = getValue(typeField, "");
                setValue(subobj, typeField, val);
                break;
            case liste:
                final int rlist = rnd.nextInt(typeField.listeValeurs.length);
                val = typeField.listeValeurs[rlist];
                val = getValue(typeField, val);
                setValue(subobj, typeField, val);
                break;
            case listeorder:
                long i = rank - lstart;
                if (i >= typeField.listeValeurs.length) {
                    i = typeField.listeValeurs.length - 1;
                }
                val = typeField.listeValeurs[(int) i];
                val = getValue(typeField, val);
                setValue(subobj, typeField, val);
                break;
            case serie:
                AtomicLong newcpt = cpt;
                long j = newcpt.get();
                if (typeField.idcpt != null) {
                    newcpt = context.cpts.get(typeField.idcpt);
                    // System.out.println("CPT replaced by: "+typeField.idcpt);
                    if (newcpt == null) {
                        newcpt = cpt;
                        LOGGER.error("wrong cpt name: " + typeField.idcpt);
                    } else {
                        j = newcpt.getAndIncrement();
                        // System.out.println("increment: "+j+" for "+typeField.name);
                    }
                }
                if (typeField.modulo > 0) {
                    j = j % typeField.modulo;
                }
                val = (typeField.prefix != null ? typeField.prefix : "") + j;
                val = getValue(typeField, val);
                setValue(subobj, typeField, val);
                break;
            case subfield:
                final BasicDBObject subobjs = new BasicDBObject();
                for (final TypeField field : typeField.subfields) {
                    final BasicDBObject obj = getDbObject(field, rank, lstart, cpt);
                    if (obj != null) {
                        subobjs.putAll((BSONObject) obj);
                    } else {
                        return null;
                    }
                }
                subobj.put(typeField.name, subobjs);
                break;
            case interval:
                final int newval = rnd.nextInt(typeField.low, typeField.high + 1);
                val = getValue(typeField, "" + newval);
                setValue(subobj, typeField, val);
                break;
            case select:
                final BasicDBObject request = new BasicDBObject();
                for (final TypeField field : typeField.subfields) {
                    final String name = field.name;
                    final String arg = getValue(field, "");
                    request.append(name, arg);
                }
                if (simulate) {
                    LOGGER.error("NotAsking: " + request);
                    return null;
                }
                final DAip maip = (DAip) MongoDbAccess.VitamCollections.Cdaip.getCollection().findOne(request,
                        new BasicDBObject(typeField.name, 1));
                if (maip != null) {
                    // System.out.println("Select: "+typeField.name+":"+ maip.get(typeField.name));
                    val = getValue(typeField, (String) maip.get(typeField.name));
                    setValue(subobj, typeField, val);
                } else {
                    // LOGGER.error("NotFound: "+request);
                    return null;
                }
                break;
            default:
                throw new InvalidExecOperationException("Incorrect type: " + typeField.type);
        }
        return subobj;
    }

    private static final String AB = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";

    private final ThreadLocalRandom rnd = ThreadLocalRandom.current();

    private final String randomString(final int len) {
        final StringBuilder sb = new StringBuilder(len);
        for (int i = 0; i < len; i++) {
            sb.append(AB.charAt(rnd.nextInt(AB.length())));
        }
        return sb.toString();
    }
}
